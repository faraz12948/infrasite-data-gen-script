const fs = require('fs');
const csvParser = require('csv-parser');
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
  MCH: "911208bf-9901-438a-bf91-862d3dffb463",
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
async function processCSV() {
  await client.connect();   // ✅ moved inside async function

  const rows = [];
  fs.createReadStream("associations.csv")
    .pipe(csvParser())
    .on("data", (row) => rows.push(row))
    .on("end", async () => {
      for (const [i, row] of rows.entries()) {
        try {
          const house = row.house;
          const rackName = row.rack;
          const floorName = row.floor;
          const roomName = row.room;
          const rackUCount = row["rack-u-count"];
          const rackMake = row["rack-make"];
          const rackModel = row["rack-model"];

          if (!houseNameIdMap[house]) {
            console.warn(`Row ${i}: Unknown house ${house}, skipping...`);
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
              `Row ${i}: No valid parent (room/floor) found for ${rackName}`
            );
            continue;
          }

          // Check rack existence in DB
          const dbRes = await client.query(
            `SELECT * FROM racks WHERE name = $1 AND parent_location_id = $2`,
            [rackName, parentId]   // ✅ use parentId, not houseId
          );

          if (dbRes.rows.length > 0) {
            console.log(
              `Row ${i}: Rack ${rackName} already exists under parent ${parentId}`
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

          console.log(`Row ${i}: Rack ${rackName} inserted successfully.`);
        } catch (err) {
          console.error(`Row ${i}: Error ->`, err.message);
        }
      }
      await client.end();
    });
}

processCSV();
