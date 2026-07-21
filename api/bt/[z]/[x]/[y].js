// Z/X/Y vector-tile endpoint backed by the buildings.pmtiles archive on
// GitHub Releases. Reads only the byte ranges for the requested tile, so the
// client never downloads the whole 124MB archive. Tiles are immutable per
// release tag, so the CDN caches them aggressively.
export const config = { runtime: 'edge' };

import { PMTiles } from "../../../_lib/pmtiles.js";

const ARCHIVE_URL =
  'https://github.com/joshgreenman1973/nyc-building-age/releases/download/tiles-v2/buildings.pmtiles';

let archive = null;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Cache-Control': 'public, max-age=3600, s-maxage=31536000, immutable',
};

export default async function handler(req) {
  const parts = new URL(req.url).pathname.split('/');
  const y = parseInt(parts.pop(), 10);
  const x = parseInt(parts.pop(), 10);
  const z = parseInt(parts.pop(), 10);

  if (!Number.isInteger(z) || !Number.isInteger(x) || !Number.isInteger(y) ||
      z < 0 || z > 22 || x < 0 || y < 0 || x >= 2 ** z || y >= 2 ** z) {
    return new Response('bad tile coords', { status: 400, headers: CORS });
  }

  if (!archive) archive = new PMTiles(ARCHIVE_URL);

  let tile;
  try {
    tile = await archive.getZxy(z, x, y);
  } catch (e) {
    archive = null; // drop possibly-poisoned instance so the next request retries
    return new Response('upstream error', { status: 502, headers: CORS });
  }

  if (!tile || !tile.data || tile.data.byteLength === 0) {
    return new Response(null, { status: 204, headers: CORS });
  }

  return new Response(tile.data, {
    status: 200,
    headers: { ...CORS, 'Content-Type': 'application/x-protobuf' },
  });
}
