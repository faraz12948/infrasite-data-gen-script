const XLSX = require('xlsx');
const { Client } = require('pg');
const axios = require('axios');

// DB client
const client = new Client({
  user: "root",
  host: "localhost",
  database: "ca_mgt",
  password: "ca-mgt",
  port: 5432,
});

const houseNameIdMap = {
  // "MCH": "911208bf-9901-438a-bf91-862d3dffb463"
  "Darshana": "11c1dee1-7390-4bfd-928e-85cbff994148"
  // add more mappings here as needed
};
// Helper: find room/floor ID
function findParentId(associationTree, floorName, roomName) {
  let parentId = null;

  function traverse(node) {
    if (node.type === "room" && node.name === roomName) {
      parentId = node.id;
      return;
    }
    if (node.type === "floor" && node.name === floorName && !parentId) {
      parentId = node.id;
      return;
    }
    if (node.children) {
      node.children.forEach(traverse);
    }
  }

  traverse(associationTree);
  return parentId;
}

// Main

async function processExcelSheets(sheetNames, excelFilePath = "NBR__logical-connectivity-data.xlsx") {
  await client.connect();
  const workbook = XLSX.readFile(excelFilePath);

  for (const sheetName of sheetNames) {
    const worksheet = workbook.Sheets[sheetName];
    if (!worksheet) {
      console.warn(`Sheet '${sheetName}' not found in file.`);
      continue;
    }
    const rows = XLSX.utils.sheet_to_json(worksheet);
    for (const [i, row] of rows.entries()) {
      try {
        const house = row.house;
        const rackName = row.rack.trim();
        const floorName = row.floor;
        const roomName = row.room;
        const rackUCount = row["rack-u-count"];
        const rackMake = row["rack-make"];
        const rackModel = row["rack-model"];

        if (!houseNameIdMap[house]) {
          console.warn(`Sheet ${sheetName} Row ${i}: Unknown house ${house}, skipping...`);
          continue;
        }

        // GET association tree
        const houseId = houseNameIdMap[house];
        const res = await axios.get(
          `http://localhost:5001/api/v1/cordinator/association/${houseId}`
        );
        const associationTree = res.data.data;

        // Find parent location (room preferred > floor)
        const parentId = findParentId(associationTree, floorName, roomName);

        if (!parentId) {
          console.warn(
            `Sheet ${sheetName} Row ${i}: No valid parent (room/floor) found for ${rackName}`
          );
          continue;
        }

        // Check rack existence in DB
        const dbRes = await client.query(
          `SELECT * FROM racks WHERE name = $1 AND parent_location_id = $2`,
          [rackName, parentId]
        );

        if (dbRes.rows.length > 0) {
          console.log(
            `Sheet ${sheetName} Row ${i}: Rack ${rackName} already exists under parent ${parentId}`
          );
          continue;
        }

        // Insert rack
        const payload = {
          name: rackName,
          make: rackMake,
          u_count: Number(rackUCount),
          model: rackModel,
          parent_location_id: parentId,
        };
        await axios.post("http://localhost:5001/api/v1/rack", payload, {
          headers: { "Content-Type": "application/json" }
        });

        console.log(`Sheet ${sheetName} Row ${i}: Rack ${rackName} inserted successfully.`);
      } catch (err) {
        console.error(`Sheet ${sheetName} Row ${i}: Error ->`, err.message);
      }
    }
  }
  await client.end();
}

// Example usage:
const sheetNames = ["structure-darshana"];
processExcelSheets(sheetNames);
