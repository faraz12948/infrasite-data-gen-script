const fs = require("fs");
const csvParser = require("csv-parser");
const { Client } = require("pg");
const axios = require("axios");

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

// Sleep helper
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Main
async function processCSV() {
  await client.connect();

  const rows = [];
  fs.createReadStream("associations.csv")
    .pipe(csvParser())
    .on("data", (row) => rows.push(row))
    .on("end", async () => {
      for (const [i, row] of rows.entries()) {
        try {
          const house = row.house?.trim();
          const rackName = row.rack?.trim();
          const rackPosition = row["rack-position"]?.trim();
          const tag = row.tag?.trim();
          const name = row.name?.trim();
          const make = row.make?.trim();
          const type = row.type?.trim();
          const model = row.model?.trim();
          const mgmtIp = row["Mgmt IP"]?.trim();
          const serviceIp = row["Service IP"]?.trim();


          if (!houseNameIdMap[house]) {
            console.warn(`Row ${i}: Unknown house ${house}, skipping...`);
            continue;
          }

          // 1. Get rack ID
          const rackRes = await client.query(
            `SELECT id 
             FROM racks 
             WHERE name = $1 
               AND institute_id::text = $2`,
            [rackName, houseNameIdMap[house]]
          );
          if (rackRes.rows.length === 0) {
            console.warn(`Row ${i}: No rack found for ${rackName}`);
            continue;
          }
          const rackId = rackRes.rows[0].id;

          // 2. Get equipment ID
          const equipRes = await client.query(
            `SELECT e.id, m.name as make, e.type 
             FROM equipments e
             JOIN make m ON m.id::text = e.make::text
             JOIN asset_type at ON at.id::text = e.type::text
             WHERE LOWER(e.model) = LOWER($1)`,
            [model]
          );
          if (equipRes.rows.length === 0) {
            console.warn(`Row ${i}: No equipment found for model ${model}`);
            continue;
          }
          const equipmentId = equipRes.rows[0].id;

          // 3. Build payload
          const payload = {
            equipments: [
              {
                id: equipmentId,
                tag: tag,
                rack_position: rackPosition,
                equipment_name: name,
                extra_device_specification: JSON.stringify(
                    type.toLowerCase() === 'server' ? { ip_service_ip: serviceIp } : 
                {
                  ip_management_ip: mgmtIp,
                }
                ),
              },
            ],
            parent_rack_id: rackId,
          };
          // 4. Post to API
          await axios.post(
            "http://localhost:5001/api/v1/equipment-rack-mapping",
            payload,
            { headers: { "Content-Type": "application/json" } }
          );

          console.log(
            `Row ${i}: Equipment ${name} mapped to rack ${rackName} successfully.`
          );

          // 5. Sleep 5 sec
          await sleep(5000);
        } catch (err) {
          console.error(`Row ${i}: Error ->`, err.message);
        }
      }
      await client.end();
    });
}

processCSV();
