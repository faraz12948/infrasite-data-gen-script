// syncAssociations.js
// Usage: set env vars PG_CONNECTION_STRING and BASE_URL then run: node syncAssociations.js

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { Client } = require('pg');
const axios = require('axios');

const CSV_PATH = path.join(__dirname, 'associations.csv');

// === Configure these (or set via env) ===
const pgConnectionString = process.env.PG_CONNECTION_STRING || 'postgresql://root:ca-mgt@localhost:5432/ca_mgt';
const baseUrl = process.env.BASE_URL || 'http://localhost:5001';
const houseNameIdMap = {
  "MCH": "911208bf-9901-438a-bf91-862d3dffb463"
  // add more mappings here as needed
};

// === Postgres client ===
const pgClient = new Client({ connectionString: pgConnectionString });


// Helper: read CSV into array of rows
function readCsv(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csv({
        mapHeaders: ({ header }) => header.trim()
      }))
      .on('data', (data) => {
        // Trim all fields
        Object.keys(data).forEach(k => data[k] = data[k] && data[k].trim());
        results.push(data);
      })
      .on('end', () => resolve(results))
      .on('error', err => reject(err));
  });
}

// Helper: Check if area exists in DB
async function areaExists(parentLocationId, areaName) {
  const sql = `SELECT 1 FROM locations WHERE type = 'area' AND parent_location_id = $1 AND name = $2 LIMIT 1`;
  const res = await pgClient.query(sql, [parentLocationId, areaName]);
  return res.rowCount > 0;
}

// Helper: Get association tree for houseId
async function getAssociationTree(houseId) {
  const url = `${baseUrl}/api/v1/cordinator/association/${encodeURIComponent(houseId)}`;
  const resp = await axios.get(url);
  if (resp && resp.data) return resp.data.data;
  throw new Error(`Invalid response from GET ${url}`);
}

// Helper: POST updated association tree
async function postAssociationTree(treePayload) {
  const url = `${baseUrl}/api/v1/cordinator/create-association-v2`;
  const resp = await axios.post(url, treePayload);
  return resp.data;
}

// Find or create High Level Diagram node in the tree object, return reference to it
function findOrCreateHighLevelDiagram(tree) {
  if (!tree.children) tree.children = [];

  let highNode = tree.children.find(c => c.name === 'High Level Diagram' && c.type === 'diagram');
  if (!highNode) {
    // create default high level node
    highNode = {
      id: null, // server may generate id or accept null; we keep null to indicate new node (depends on API)
      name: 'High Level Diagram',
      type: 'diagram',
      summary: { list: [], type: 'house', title: '', description: '' },
      parentId: tree.parentId || null,
      description: '',
      children: []
    };
    tree.children.unshift(highNode); // add at beginning
  } else if (!highNode.children) {
    highNode.children = [];
  }

  return highNode;
}

// Check if an area with the same name already exists under a diagram node (by name)
function hasArea(diagramNode, areaName) {
  return (diagramNode.children || []).some(c => c.type === 'area' && c.name === areaName);
}

// Helper: check if value is invalid (empty, N/A, null etc.)
function isInvalid(value) {
  if (!value) return true;
  const badValues = ['n/a', 'na', 'null'];
  return badValues.includes(value.trim().toLowerCase());
}

// Build nested structure dynamically, skipping invalid levels + children
function buildNestedStructure(areaName, buildingName, floorName, roomName) {
  // Area is mandatory: if invalid -> return null
  if (isInvalid(areaName)) return null;

  const areaNode = {
    name: areaName,
    type: 'area',
    summary: { list: [{ label: '', value: '' }], type: '', title: '', description: '' },
    description: '',
    children: []
  };

  // Building
  if (!isInvalid(buildingName)) {
    const buildingNode = {
      name: buildingName,
      type: 'building',
      summary: { list: [{ label: '', value: '' }], type: '', title: '', description: '' },
      description: '',
      children: []
    };

    // Floor
    if (!isInvalid(floorName)) {
      const floorNode = {
        name: floorName,
        type: 'floor',
        summary: { list: [{ label: '', value: '' }], type: '', title: '', description: '' },
        description: '',
        children: []
      };

      // Room
      if (!isInvalid(roomName)) {
        const roomNode = {
          name: roomName,
          type: 'room',
          summary: { list: [{ label: '', value: '' }], type: '', title: '', description: '' },
          description: ''
        };
        floorNode.children.push(roomNode);
      }

      buildingNode.children.push(floorNode);
    }

    areaNode.children.push(buildingNode);
  }

  return areaNode;
}


// Main flow
(async function main() {
  try {
    console.log('Connecting to Postgres...');
    await pgClient.connect();

    console.log(`Reading CSV from ${CSV_PATH} ...`);
    const rows = await readCsv(CSV_PATH);
    console.log(`Found ${rows.length} rows.`);

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      // expected columns: include, house, area, building, floor, room
      const include = (row.include || '').toLowerCase();
      if (include !== 'yes') {
        console.log(`Row ${i}: include != 'Yes' -> skipping`);
        continue;
      }

      const houseKey = row.house;
      if (!houseKey) {
        console.warn(`Row ${i}: missing house value -> skipping`);
        continue;
      }

      const houseId = houseNameIdMap[houseKey];
      if (!houseId) {
        console.warn(`Row ${i}: house "${houseKey}" not mapped in houseNameIdMap -> skipping`);
        continue;
      }

      const areaName = row.area;
      const buildingName = row.building;
      const floorName = row.floor;
      const roomName = row.room;

      if (!areaName || !buildingName || !floorName || !roomName) {
        console.warn(`Row ${i}: missing one of area/building/floor/room -> skipping`);
        continue;
      }

      console.log(`Row ${i}: processing house=${houseKey}(${houseId}), area="${areaName}"`);

      // Step 3-5: check psql if area already exists under parent = houseId
      let exists = false;
      try {
        exists = await areaExists(houseId, areaName);
      } catch (err) {
        console.error(`Row ${i}: Postgres check failed:`, err.message);
        // decide to skip or continue - we'll skip this row to be safe
        continue;
      }

      if (exists) {
        console.log(`Row ${i}: area "${areaName}" found in DB under parent ${houseId} -> skipping insertion`);
        continue;
      }

      // Step 6: GET association tree for house
      let tree;
      try {
        tree = await getAssociationTree(houseId);
      } catch (err) {
        console.error(`Row ${i}: GET association failed:`, err.message);
        continue;
      }

      // Step 7: find High Level Diagram block (or create it)
      const highNode = findOrCreateHighLevelDiagram(tree);

      // Step 8: insert area->building->floor->room under highNode
      if (hasArea(highNode, areaName)) {
        console.log(`Row ${i}: area "${areaName}" already exists under High Level Diagram -> skipping add`);
        // (Note: DB said area didn't exist under house root, but maybe exists under HLD — still skip.)
        continue;
      }

      const areaNode = buildNestedStructure(areaName, buildingName, floorName, roomName);
      // Optionally set parentId for area node to highNode.id if available
      if (highNode.id) areaNode.parentId = highNode.id;

      highNode.children.push(areaNode);

      // Step 9: POST updated association tree
      const payload = {
        // The example POST expects a wrapper with messageCode success etc in response, but we should
        // send the tree object as-is under body (example shows full tree). We'll send `{ parentId: ..., children: [...] }`.
        parentId: tree.parentId,
        children: tree.children
      };

      try {
        const postResp = await postAssociationTree({ parentId: payload.parentId, children: payload.children });
        console.log(`Row ${i}: POST result:`, postResp && postResp.messageCode ? postResp.messageCode : 'OK');
      } catch (err) {
        console.error(`Row ${i}: POST failed:`, err.message || err);
        // optionally remove the areaNode we pushed so next attempt doesn't double-add in-memory
        // but since we're skipping duplicates by DB next run, it's fine. We'll continue.
      }

      // small delay to avoid hammering local API (optional)
      await new Promise(r => setTimeout(r, 200)); // 200ms
    }

    console.log('Done processing CSV.');
  } catch (err) {
    console.error('Fatal error:', err);
  } finally {
    try { await pgClient.end(); } catch (e) {}
  }
})();
