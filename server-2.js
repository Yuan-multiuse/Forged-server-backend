require('dotenv').config();
const express    = require('express');
const cors       = require('cors');
const helmet     = require('helmet');
const session    = require('express-session');
const PgSession  = require('connect-pg-simple')(session);
const { Pool }   = require('pg');
const { Client, GatewayIntentBits } = require('discord.js');
const WebSocket  = require('ws');

const app  = express();
const PORT = process.env.PORT || 3000;

// ── PostgreSQL ───────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false,
});

// ── Discord Bot ──────────────────────────────────────────
const discord = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMembers,
            GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent],
});
var discordReady = false;
var GUILD_ID           = process.env.DISCORD_GUILD_ID;
var INVITE_CHANNEL_ID  = process.env.DISCORD_INVITE_CHANNEL_ID;
var WELCOME_CHANNEL_ID = process.env.DISCORD_WELCOME_CHANNEL_ID;
var INVITE_LINK        = process.env.DISCORD_INVITE_LINK || 'https://discord.gg/q2xGegbuvW';

discord.once('ready', () => { discordReady = true; console.log('✅ Discord bot:', discord.user.tag); });
if (process.env.DISCORD_BOT_TOKEN) {
  discord.login(process.env.DISCORD_BOT_TOKEN).catch(e => console.error('Discord login failed:', e.message));
}

// ══════════════════════════════════════════════════════════
//  MINECRAFT WEBSOCKET BRIDGE
//  Connects to the Bedrock addon's WebSocket server
//  Address: cultivatorsmp.minecra.fr:25801 (WS port set in addon)
// ══════════════════════════════════════════════════════════
const MC_WS_URL    = process.env.MC_WS_URL    || 'ws://cultivatorsmp.minecra.fr:8080';
const WS_SECRET    = process.env.WS_SECRET    || 'forged-ws-secret-2025';
const WS_RECONNECT = 10000; // retry every 10s

var mcWs           = null;
var mcConnected    = false;
var mcPlayers      = [];       // cached online player list
var mcReconnTimer  = null;
var pendingCmds    = {};       // { reqId: { resolve, reject, timeout } }
var cmdIdCounter   = 1;

function connectToMinecraft() {
  if (mcWs) { try { mcWs.terminate(); } catch(e){} }

  console.log('Connecting to Minecraft WS:', MC_WS_URL);
  mcWs = new WebSocket(MC_WS_URL);

  mcWs.on('open', function() {
    mcConnected = true;
    console.log('✅ Minecraft WS connected');
    clearTimeout(mcReconnTimer);
    // Authenticate
    mcWs.send(JSON.stringify({ type: 'auth', secret: WS_SECRET }));
    // Request player list immediately
    sendMcCommand('get_players', {});
  });

  mcWs.on('message', function(raw) {
    try {
      var msg = JSON.parse(raw.toString());
      // Resolve pending command response
      if (msg.reqId && pendingCmds[msg.reqId]) {
        clearTimeout(pendingCmds[msg.reqId].timeout);
        pendingCmds[msg.reqId].resolve(msg);
        delete pendingCmds[msg.reqId];
        return;
      }
      // Push events
      if (msg.type === 'players_update') {
        mcPlayers = msg.players || [];
      }
      if (msg.type === 'player_join' || msg.type === 'player_leave') {
        sendMcCommand('get_players', {}); // refresh list
      }
    } catch(e) { console.error('WS parse error:', e.message); }
  });

  mcWs.on('close', function() {
    mcConnected = false;
    mcPlayers   = [];
    console.log('Minecraft WS disconnected — retrying in', WS_RECONNECT/1000, 's');
    mcReconnTimer = setTimeout(connectToMinecraft, WS_RECONNECT);
  });

  mcWs.on('error', function(e) {
    console.error('Minecraft WS error:', e.message);
    mcConnected = false;
  });
}

// Send a command and optionally wait for response
function sendMcCommand(type, payload, waitResponse) {
  return new Promise(function(resolve, reject) {
    if (!mcConnected || !mcWs) return resolve({ ok: false, error: 'not_connected' });
    var reqId = 'cmd_' + (cmdIdCounter++);
    var msg   = Object.assign({ type, reqId }, payload);
    try {
      mcWs.send(JSON.stringify(msg));
      if (!waitResponse) return resolve({ ok: true, reqId });
      // Wait up to 5s for response
      var t = setTimeout(function() {
        delete pendingCmds[reqId];
        resolve({ ok: false, error: 'timeout' });
      }, 5000);
      pendingCmds[reqId] = { resolve, reject, timeout: t };
    } catch(e) {
      resolve({ ok: false, error: e.message });
    }
  });
}

// Start connecting
connectToMinecraft();

// ── Middleware ───────────────────────────────────────────
app.set('trust proxy', 1);
app.use(helmet({ contentSecurityPolicy: false }));
const allowedOrigins = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
app.use(cors({
  origin: (origin, cb) => (!origin || !allowedOrigins.length || allowedOrigins.includes(origin)) ? cb(null,true) : cb(new Error('CORS')),
  credentials: true,
}));
app.use(express.json());
app.use(session({
  store: new PgSession({ pool, tableName: 'session' }),
  secret: process.env.SESSION_SECRET || 'forged-secret-change-me',
  resave: false, saveUninitialized: false,
  cookie: { secure: process.env.NODE_ENV==='production', httpOnly:true, maxAge:7*24*60*60*1000, sameSite: process.env.NODE_ENV==='production'?'none':'lax' },
}));

// ── DB Init ──────────────────────────────────────────────
async function initDB() {
  await pool.query(`CREATE TABLE IF NOT EXISTS session (sid VARCHAR NOT NULL COLLATE "default", sess JSON NOT NULL, expire TIMESTAMP(6) NOT NULL, CONSTRAINT session_pkey PRIMARY KEY(sid)); CREATE INDEX IF NOT EXISTS idx_session_expire ON session(expire);`);
  await pool.query(`CREATE TABLE IF NOT EXISTS posts (id SERIAL PRIMARY KEY, gamertag VARCHAR(25) NOT NULL, uuid VARCHAR(36), edition VARCHAR(16) DEFAULT 'unknown', discord_user VARCHAR(40), discord_status VARCHAR(16) DEFAULT 'unchecked', content TEXT NOT NULL CHECK(char_length(content)<=500), created_at TIMESTAMPTZ DEFAULT NOW()); CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_at DESC);`);
  await pool.query(`CREATE TABLE IF NOT EXISTS comments (id SERIAL PRIMARY KEY, post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE, gamertag VARCHAR(25) NOT NULL, uuid VARCHAR(36), discord_user VARCHAR(40), content TEXT NOT NULL CHECK(char_length(content)<=280), created_at TIMESTAMPTZ DEFAULT NOW()); CREATE INDEX IF NOT EXISTS idx_comments_post ON comments(post_id,created_at);`);
  await pool.query(`CREATE TABLE IF NOT EXISTS likes (gamertag VARCHAR(25) NOT NULL, post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE, PRIMARY KEY(gamertag,post_id));`);
  await pool.query(`CREATE TABLE IF NOT EXISTS comment_likes (gamertag VARCHAR(25) NOT NULL, comment_id INTEGER REFERENCES comments(id) ON DELETE CASCADE, PRIMARY KEY(gamertag,comment_id));`);
  console.log('✅ DB ready');
}

function timeAgo(d) {
  const s=Math.floor((Date.now()-new Date(d))/1000);
  if(s<60)return'just now'; if(s<3600)return Math.floor(s/60)+'m ago';
  if(s<86400)return Math.floor(s/3600)+'h ago'; return Math.floor(s/86400)+'d ago';
}

function getPlayer(req) {
  const gt=req.body?.gamertag||req.query?.gamertag;
  if(!gt||typeof gt!=='string'||gt.length<2||gt.length>25) return null;
  if(!/^[a-zA-Z0-9_ ]+$/.test(gt.trim())) return null;
  return { gamertag:gt.trim(), uuid:req.body?.uuid||null, edition:req.body?.edition||'unknown',
           discordUser:req.body?.discordUser||null, discordStatus:req.body?.discordStatus||'unchecked' };
}

// ══════════════════════════════════════════════════════════
//  SERVER STATUS ENDPOINTS
// ══════════════════════════════════════════════════════════

// GET /api/server/status — returns live player list
app.get('/api/server/status', async (req, res) => {
  if (!mcConnected) {
    return res.json({ online: false, players: [], count: 0, address: 'cultivatorsmp.minecra.fr:25801' });
  }
  // Refresh from game
  var result = await sendMcCommand('get_players', {}, true);
  if (result.players) mcPlayers = result.players;
  res.json({
    online:  true,
    players: mcPlayers,
    count:   mcPlayers.length,
    address: 'cultivatorsmp.minecra.fr:25801',
  });
});

// POST /api/server/command — website triggers in-game action
app.post('/api/server/command', async (req, res) => {
  const { command, gamertag } = req.body;
  if (!command) return res.status(400).json({ error: 'command required' });

  if (!mcConnected) {
    return res.json({ ok: false, error: 'Server offline' });
  }

  switch (command) {
    case 'web_login':
      // Send welcome message in-game
      await sendMcCommand('send_message', {
        message: `§6[FORGE]§r §e${gamertag}§r has entered the Covenant website. ⚔`,
      });
      break;

    case 'give_yuan':
      // Admin only — give yuan to player
      if (!req.body.target || !req.body.amount) return res.status(400).json({ error: 'target and amount required' });
      await sendMcCommand('give_currency', {
        target:   req.body.target,
        currency: 'yuan',
        amount:   parseInt(req.body.amount),
      }, true);
      break;

    case 'give_shards':
      if (!req.body.target || !req.body.amount) return res.status(400).json({ error: 'target and amount required' });
      await sendMcCommand('give_currency', {
        target:   req.body.target,
        currency: 'shards',
        amount:   parseInt(req.body.amount),
      }, true);
      break;

    case 'whitelist':
      // Whitelist player when they log into website
      await sendMcCommand('whitelist_add', { gamertag });
      break;

    case 'announcement':
      if (!req.body.message) return res.status(400).json({ error: 'message required' });
      await sendMcCommand('send_message', {
        message: `§6[FORGE]§r ${req.body.message}`,
      });
      break;

    default:
      return res.status(400).json({ error: 'unknown command' });
  }

  res.json({ ok: true });
});

// ── DISCORD CHECK ─────────────────────────────────────────
app.post('/api/discord/check', async (req, res) => {
  const { discordUsername, gamertag } = req.body;
  if (!discordUsername) return res.status(400).json({ error: 'discordUsername required' });
  if (!discordReady || !GUILD_ID) return res.json({ isMember: true, message: 'Discord offline' });

  try {
    const guild  = await discord.guilds.fetch(GUILD_ID);
    await guild.members.fetch().catch(()=>{});
    const uname  = discordUsername.trim().toLowerCase();
    const member = guild.members.cache.find(m =>
      m.user.username.toLowerCase() === (uname.includes('#') ? uname.split('#')[0] : uname) ||
      (m.nickname && m.nickname.toLowerCase() === uname)
    );

    if (member) {
      if (WELCOME_CHANNEL_ID) {
        const ch = await guild.channels.fetch(WELCOME_CHANNEL_ID).catch(()=>null);
        if (ch?.isTextBased()) {
          await ch.send(`⚔ **${gamertag||discordUsername}** has entered **The Forge**! Welcome back, ${member.toString()} 🔥`).catch(()=>{});
        }
      }
      return res.json({ isMember:true, discordId:member.id, nickname:member.nickname||member.user.username, message:'Member confirmed' });
    }

    const invCh = INVITE_CHANNEL_ID ? await guild.channels.fetch(INVITE_CHANNEL_ID).catch(()=>null) : guild.systemChannel;
    if (invCh?.isTextBased()) {
      await invCh.send(`📨 **${gamertag||discordUsername}** tried to enter The Forge but isn't in the server!\nHey **${discordUsername}** — join here: **${INVITE_LINK}**`).catch(()=>{});
    }
    return res.json({ isMember:false, invited:true, inviteUrl:INVITE_LINK, message:`Not in Discord yet — join: ${INVITE_LINK}` });
  } catch(e) {
    return res.json({ isMember:true, message:'Discord error — proceeding' });
  }
});

// ── POSTS ─────────────────────────────────────────────────
app.get('/api/posts', async (req,res) => {
  const gt=req.query?.gamertag?.trim()||null, page=Math.max(1,parseInt(req.query.page)||1), limit=20, offset=(page-1)*limit;
  try {
    const r=await pool.query(`SELECT p.id,p.gamertag,p.uuid,p.edition,p.discord_user,p.discord_status,p.content,p.created_at,COUNT(DISTINCT l.gamertag)::int AS like_count,COUNT(DISTINCT c.id)::int AS comment_count,${gt?"EXISTS(SELECT 1 FROM likes WHERE post_id=p.id AND gamertag=$3) AS liked":"false AS liked"} FROM posts p LEFT JOIN likes l ON l.post_id=p.id LEFT JOIN comments c ON c.post_id=p.id GROUP BY p.id ORDER BY p.created_at DESC LIMIT $1 OFFSET $2`,gt?[limit,offset,gt]:[limit,offset]);
    const total=(await pool.query('SELECT COUNT(*) FROM posts')).rows[0].count;
    res.json({posts:r.rows.map(p=>({id:p.id,gamertag:p.gamertag,uuid:p.uuid,edition:p.edition,discordUser:p.discord_user,discordStatus:p.discord_status,content:p.content,time:timeAgo(p.created_at),likeCount:p.like_count,commentCount:p.comment_count,liked:p.liked,avatarUrl:p.uuid?`https://crafatar.com/avatars/${p.uuid}?size=64&overlay`:null})),page,total:parseInt(total),pages:Math.ceil(parseInt(total)/limit)});
  } catch(e){res.status(500).json({error:'DB error'});}
});
app.post('/api/posts', async (req,res) => {
  const p=getPlayer(req); if(!p) return res.status(401).json({error:'Invalid gamertag'});
  const {content}=req.body; if(!content?.trim()||content.length>500) return res.status(400).json({error:'Invalid content'});
  try {
    const r=await pool.query(`INSERT INTO posts(gamertag,uuid,edition,discord_user,discord_status,content)VALUES($1,$2,$3,$4,$5,$6)RETURNING *`,[p.gamertag,p.uuid,p.edition,p.discordUser,p.discordStatus,content.trim()]);
    const x=r.rows[0];
    res.json({id:x.id,gamertag:x.gamertag,uuid:x.uuid,content:x.content,time:'just now',likeCount:0,commentCount:0,liked:false,avatarUrl:x.uuid?`https://crafatar.com/avatars/${x.uuid}?size=64&overlay`:null});
  } catch(e){res.status(500).json({error:'DB error'});}
});
app.delete('/api/posts/:id', async (req,res) => {
  const p=getPlayer(req); if(!p) return res.status(401).json({error:'Invalid gamertag'});
  const r=await pool.query('DELETE FROM posts WHERE id=$1 AND gamertag=$2 RETURNING id',[req.params.id,p.gamertag]);
  if(!r.rowCount) return res.status(404).json({error:'Not found'}); res.json({ok:true});
});
app.post('/api/posts/:id/like', async (req,res) => {
  const p=getPlayer(req); if(!p) return res.status(401).json({error:'Invalid gamertag'});
  try {
    const ex=await pool.query('SELECT 1 FROM likes WHERE post_id=$1 AND gamertag=$2',[req.params.id,p.gamertag]);
    if(ex.rowCount>0) await pool.query('DELETE FROM likes WHERE post_id=$1 AND gamertag=$2',[req.params.id,p.gamertag]);
    else await pool.query('INSERT INTO likes(post_id,gamertag)VALUES($1,$2)',[req.params.id,p.gamertag]);
    const cnt=await pool.query('SELECT COUNT(*)::int FROM likes WHERE post_id=$1',[req.params.id]);
    res.json({liked:ex.rowCount===0,count:cnt.rows[0].count});
  } catch(e){res.status(500).json({error:'DB error'});}
});
app.get('/api/posts/:id/comments', async (req,res) => {
  const gt=req.query?.gamertag?.trim()||null;
  try {
    const r=await pool.query(`SELECT c.id,c.gamertag,c.uuid,c.discord_user,c.content,c.created_at,COUNT(cl.gamertag)::int AS like_count,${gt?"EXISTS(SELECT 1 FROM comment_likes WHERE comment_id=c.id AND gamertag=$2) AS liked":"false AS liked"} FROM comments c LEFT JOIN comment_likes cl ON cl.comment_id=c.id WHERE c.post_id=$1 GROUP BY c.id ORDER BY c.created_at ASC`,gt?[req.params.id,gt]:[req.params.id]);
    res.json(r.rows.map(c=>({id:c.id,gamertag:c.gamertag,uuid:c.uuid,discordUser:c.discord_user,content:c.content,time:timeAgo(c.created_at),likeCount:c.like_count,liked:c.liked,avatarUrl:c.uuid?`https://crafatar.com/avatars/${c.uuid}?size=64&overlay`:null})));
  } catch(e){res.status(500).json({error:'DB error'});}
});
app.post('/api/posts/:id/comments', async (req,res) => {
  const p=getPlayer(req); if(!p) return res.status(401).json({error:'Invalid gamertag'});
  const {content}=req.body; if(!content?.trim()||content.length>280) return res.status(400).json({error:'Invalid content'});
  try {
    const r=await pool.query('INSERT INTO comments(post_id,gamertag,uuid,discord_user,content)VALUES($1,$2,$3,$4,$5)RETURNING *',[req.params.id,p.gamertag,p.uuid,p.discordUser,content.trim()]);
    const c=r.rows[0];
    res.json({id:c.id,gamertag:c.gamertag,uuid:c.uuid,content:c.content,time:'just now',likeCount:0,liked:false,avatarUrl:c.uuid?`https://crafatar.com/avatars/${c.uuid}?size=64&overlay`:null});
  } catch(e){res.status(500).json({error:'DB error'});}
});
app.post('/api/comments/:id/like', async (req,res) => {
  const p=getPlayer(req); if(!p) return res.status(401).json({error:'Invalid gamertag'});
  try {
    const ex=await pool.query('SELECT 1 FROM comment_likes WHERE comment_id=$1 AND gamertag=$2',[req.params.id,p.gamertag]);
    if(ex.rowCount>0) await pool.query('DELETE FROM comment_likes WHERE comment_id=$1 AND gamertag=$2',[req.params.id,p.gamertag]);
    else await pool.query('INSERT INTO comment_likes(comment_id,gamertag)VALUES($1,$2)',[req.params.id,p.gamertag]);
    const cnt=await pool.query('SELECT COUNT(*)::int FROM comment_likes WHERE comment_id=$1',[req.params.id]);
    res.json({liked:ex.rowCount===0,count:cnt.rows[0].count});
  } catch(e){res.status(500).json({error:'DB error'});}
});

app.get('/health', (req,res) => res.json({ ok:true, discord:discordReady, minecraft:mcConnected, players:mcPlayers.length }));

initDB().then(()=>app.listen(PORT,()=>console.log(`🔥 Forged SMP API on port ${PORT}`))).catch(e=>{console.error(e);process.exit(1);});
