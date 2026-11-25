// Race in the Fast Lane! — live car server (Cloudflare Worker + Durable Object)
// Every player opens one WebSocket here. The server passes each car's position on to everyone else.
// It keeps no history: when everyone leaves, it goes to sleep and costs nothing.

const MAX_PLAYERS = 60;      // per town
const MAX_MSG = 2000;        // bytes; a car update is ~250

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/' || url.pathname === '/health') {
      return new Response('Race in the Fast Lane! live server is running ♥', { headers: { 'content-type': 'text/plain; charset=utf-8' } });
    }
    if (url.pathname === '/town') {
      if (request.headers.get('Upgrade') !== 'websocket') return new Response('Expected a WebSocket', { status: 426 });
      const id = env.TOWN.idFromName('main');
      return env.TOWN.get(id).fetch(request);
    }
    return new Response('Not found', { status: 404 });
  }
};

export class Town {
  constructor(ctx, env) { this.ctx = ctx; }

  async fetch(request) {
    const socks = this.ctx.getWebSockets();
    if (socks.length >= MAX_PLAYERS) return new Response('Town is full', { status: 503 });
    const pair = new WebSocketPair(), [client, server] = Object.values(pair);
    const id = crypto.randomUUID().slice(0, 8);
    this.ctx.acceptWebSocket(server);                       // hibernation API: no cost while idle
    server.serializeAttachment({ id, s: null });
    // tell the newcomer who is already here
    const snap = [];
    for (const ws of socks) { const a = ws.deserializeAttachment(); if (a && a.s) snap.push({ i: a.id, s: a.s }); }
    server.send(JSON.stringify({ hello: id, snap }));
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, data) {
    if (typeof data !== 'string' || data.length > MAX_MSG) return;
    let m; try { m = JSON.parse(data); } catch (e) { return; }
    if (!m || typeof m.s !== 'object' || m.s === null) return;
    const a = ws.deserializeAttachment() || {};
    a.s = m.s; ws.serializeAttachment(a);
    const out = JSON.stringify({ i: a.id, s: m.s });
    for (const other of this.ctx.getWebSockets()) if (other !== ws) { try { other.send(out); } catch (e) {} }
  }

  async webSocketClose(ws) { this.bye(ws); }
  async webSocketError(ws) { this.bye(ws); }

  bye(ws) {
    const a = ws.deserializeAttachment(); if (!a) return;
    const out = JSON.stringify({ i: a.id, bye: 1 });
    for (const other of this.ctx.getWebSockets()) if (other !== ws) { try { other.send(out); } catch (e) {} }
    try { ws.close(1000, 'bye'); } catch (e) {}
  }
}
