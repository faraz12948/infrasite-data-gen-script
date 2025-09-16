// syncAssociations.js
// Usage: set env vars PG_CONNECTION_STRING and BASE_URL then run: node syncAssociations.js

const XLSX = require('xlsx');
const path = require('path');
const { Client } = require('pg');
const axios = require('axios');

const EXCEL_PATH = path.join(__dirname, 'NBR__logical-connectivity-data.xlsx');

// === Configure these (or set via env) ===
const pgConnectionString = process.env.PG_CONNECTION_STRING || 'postgresql://root:ca-mgt@localhost:5432/ca_mgt';
const baseUrl = process.env.BASE_URL || 'http://localhost:5001';
const houseNameIdMap = {
  // "MCH": "911208bf-9901-438a-bf91-862d3dffb463"
  "Darshana": "11c1dee1-7390-4bfd-928e-85cbff994148"
  // add more mappings here as needed
};

// === Postgres client ===
const pgClient = new Client({ connectionString: pgConnectionString });


// Helper: read Excel sheet into array of rows
function readSheet(sheet, workbook) {
  const worksheet = workbook.Sheets[sheet];
  if (!worksheet) return [];
  const rows = XLSX.utils.sheet_to_json(worksheet);
  // Trim all fields
  return rows.map(row => {
    Object.keys(row).forEach(k => row[k] = row[k] && row[k].toString().trim());
    return row;
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
  const badValues = ['n/a', 'na', 'null','-'];
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

// Main flow for Excel sheets
(async function main() {
  try {
    console.log('Connecting to Postgres...');
    await pgClient.connect();

    const sheetNames = ["structure-darshana"];
    const workbook = XLSX.readFile(EXCEL_PATH);

    for (const sheetName of sheetNames) {
      console.log(`Reading sheet: ${sheetName}`);
      const rows = readSheet(sheetName, workbook);
      console.log(`Found ${rows.length} rows in ${sheetName}.`);

      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        // expected columns: include, house, area, building, floor, room
        const include = (row.include || '').toLowerCase();
        if (include !== 'yes') {
          console.log(`Sheet ${sheetName} Row ${i}: include != 'Yes' -> skipping`);
          continue;
        }

        const houseKey = row.house;
        if (!houseKey) {
          console.warn(`Sheet ${sheetName} Row ${i}: missing house value -> skipping`);
          continue;
        }

        const houseId = houseNameIdMap[houseKey];
        if (!houseId) {
          console.warn(`Sheet ${sheetName} Row ${i}: house "${houseKey}" not mapped in houseNameIdMap -> skipping`);
          continue;
        }

        const areaName = row.area;
        const buildingName = row.building;
        const floorName = row.floor;
        const roomName = row.room;

        if (!areaName || !buildingName || !floorName || !roomName) {
          console.warn(`Sheet ${sheetName} Row ${i}: missing one of area/building/floor/room -> skipping`);
          continue;
        }

        console.log(`Sheet ${sheetName} Row ${i}: processing house=${houseKey}(${houseId}), area="${areaName}"`);

        // Step 3-5: check psql if area already exists under parent = houseId
        let exists = false;
        try {
          exists = await areaExists(houseId, areaName);
        } catch (err) {
          console.error(`Sheet ${sheetName} Row ${i}: Postgres check failed:`, err.message);
          continue;
        }

        if (exists) {
          console.log(`Sheet ${sheetName} Row ${i}: area "${areaName}" found in DB under parent ${houseId} -> skipping insertion`);
          continue;
        }

        // Step 6: GET association tree for house
        let tree;
        try {
          tree = await getAssociationTree(houseId);
        } catch (err) {
          console.error(`Sheet ${sheetName} Row ${i}: GET association failed:`, err.message);
          continue;
        }

        // Step 7: find High Level Diagram block (or create it)
        const highNode = findOrCreateHighLevelDiagram(tree);

        // Step 8: insert area->building->floor->room under highNode
        if (hasArea(highNode, areaName)) {
          console.log(`Sheet ${sheetName} Row ${i}: area "${areaName}" already exists under High Level Diagram -> skipping add`);
          continue;
        }

        const areaNode = buildNestedStructure(areaName, buildingName, floorName, roomName);
        if (highNode.id) areaNode.parentId = highNode.id;

        highNode.children.push(areaNode);

        // Step 9: POST updated association tree
        const payload = {
          parentId: tree.parentId,
          children: tree.children
        };

        try {
          const postResp = await postAssociationTree({ parentId: payload.parentId, children: payload.children });
          console.log(`Sheet ${sheetName} Row ${i}: POST result:`, postResp && postResp.messageCode ? postResp.messageCode : 'OK');
        } catch (err) {
          console.error(`Sheet ${sheetName} Row ${i}: POST failed:`, err.message || err);
        }

        await new Promise(r => setTimeout(r, 200)); // 200ms
      }
    }

    console.log('Done processing Excel sheets.');
  } catch (err) {
    console.error('Fatal error:', err);
  } finally {
    try { await pgClient.end(); } catch (e) {}
  }
})();
