export const config = { runtime: 'edge' };

const RELEASE_BASE = 'https://github.com/joshgreenman1973/nyc-building-age/releases/download/tiles-v1';

export default async function handler(req) {
  const url = new URL(req.url);
  const file = url.pathname.split('/').pop();

  const upstreamHeaders = {};
  const range = req.headers.get('range');
  if (range) upstreamHeaders['Range'] = range;

  const upstream = await fetch(`${RELEASE_BASE}/${file}`, {
    headers: upstreamHeaders,
    redirect: 'follow',
  });

  const headers = new Headers();
  // Copy relevant upstream headers
  const passthrough = ['content-type', 'content-length', 'content-range', 'accept-ranges', 'etag', 'last-modified'];
  for (const name of passthrough) {
    const v = upstream.headers.get(name);
    if (v) headers.set(name, v);
  }
  // CORS
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Headers', 'Range');
  headers.set('Access-Control-Expose-Headers', 'Content-Range, Accept-Ranges, Content-Length');
  headers.set('Cache-Control', 'public, max-age=3600');
  // Ensure ranges are supported in case upstream didn't set it
  if (!headers.has('accept-ranges')) headers.set('accept-ranges', 'bytes');

  return new Response(upstream.body, {
    status: upstream.status,
    headers,
  });
}
