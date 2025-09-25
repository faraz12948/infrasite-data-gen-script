const XLSX = require("xlsx");
const { Client } = require("pg");
const axios = require("axios");

// DB client
const client = new Client({
  user: "root",
  host: "localhost",
  database: "ca_mgt_v4",
  password: "ca-mgt",
  port: 5432,
});

const houseNameIdMap = {
  DR: "9745ec1d-7cc3-444a-b2f9-0196de9330ce",
  DC: "cf02adf7-0408-40a7-a4cb-0de18652dbc9",
  NDC: "ae612ba3-6f10-4308-921d-418c90bb96ff",
  CCH: "92550f0f-f3ae-4883-8199-8321702aa610",
  "NBR-New-Bldg": "3ae8c5d4-4803-44c2-87d3-3eda1cbf6fd0",
  DCH: "8191e270-5870-437e-9d5e-f165c6e37ec8",
  MCH: "20ee539d-ddbf-484a-b292-73df53ce1907",
  ICD: "ed4c2458-d8d1-41a4-a666-081d80dc44e0",
  BCH: "ebc70362-c841-41ad-88c4-c35dfa6a52c6",
  PCH: "79469b7b-2cea-4053-969e-007175a154ff",
  "CCH Bond": "11752b76-2a75-41fb-acf5-62dcb64c90dd",
  "Dhaka Bond": "8f475c68-08ff-41d8-ad96-fa466c8ce62b",
  Adamjee: "7beac6af-1d14-42fa-8396-7d692d548a75",
  "Uttara EPZ": "9a0f60a9-699c-47f8-9694-a7cf4f9831c9",
  Dhaka_EPZ: "e37277e1-cccd-472e-a858-52e0722f53d5",
  CEPZ: "c4220097-ba72-4135-a599-f0a04efe0bb6",
  Darshana: "4b7b47b5-d50e-41a7-b6aa-28ec37b084e4",
  Bhomra: "60bc421e-a538-461b-a7b5-ba6462cba1df",
  Banglabandha: "a48109ed-b0e0-44d7-b00b-6eff4d76b951",
  Hilli: "dfa39f2e-c3e1-4653-b465-4bf8da92e4c6",
  Burimari: "3198c784-32d2-4ebf-901b-e2758d9c41ea",
  Sonamasjid: "1f49bca9-2891-4f1e-af08-f39fa90f3bcc",
  Teknaf: "b7266c14-7e3f-436a-8fcf-55abdfdbad67",
  Akhawra: "36c96ec0-4689-42cf-b23b-b8a7fb8194b0",
  Rohanpur: "e09f5906-0fe7-4fe6-95ee-6fdfab013fd8",
  Tamabil: "d22eb602-9d18-4de4-9014-f522e5f67285",
  Shonahut: "57b71ae8-ef34-4cb4-a5f5-08ff3bef6fa7",
  Shewla: "05eb7586-8bb7-4172-a5d5-433e30294115",
  Dhanua: "6ee7c4fd-f714-48d2-b77e-f6434f37fc5d",
  "Bibir Bazar": "3298f3b1-71c2-408e-8a7b-cb004d99bf16",
};

// Sleep helper
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Logger setup for file output with colored errors/warnings
const { initLogger } = require("../logger");
initLogger("generate-rack");

// Main
const specGenerator = (type, mgmtIp, serviceIp, clusterIp, os) => {
  if (type?.toLowerCase() === "server") {
    return JSON.stringify({
      ip_service_ip: serviceIp,
      operating_system_flavor: os,
    });
  } else if (type?.toLowerCase() === "storage") {
    return JSON.stringify({ ip_cluster_ip: clusterIp });
  } else {
    return JSON.stringify({ ip_management_ip: mgmtIp });
  }
};
async function processExcelSheets(
  sheetNames,
  excelFilePath = "../NBR__logical-connectivity-data.xlsx"
) {
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
        const house = row.house?.trim();
        const rackName = row.rack?.trim();
        const rackPosition = row["rack-position"].toString();
        const tag = row.tag != null ? String(row.tag).trim() : undefined;
        const name = row.name?.trim();
        const make = row.make?.trim();
        const type = row.type?.trim();
        const model = row.model?.trim();
        const mgmtIp = row["Mgmt IP"]?.trim();
        const serviceIp = row["Service IP"]?.trim();
        const clusterIp = row["Cluster IP"]?.trim();
        const os = row["OS"]?.trim();

        if (!houseNameIdMap[house]) {
          console.warn(
            `Sheet ${sheetName} Row ${i}: Unknown house ${house}, skipping...`
          );
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
          console.warn(
            `Sheet ${sheetName} Row ${i}: No rack found for ${rackName}`
          );
          continue;
        }
        const rackId = rackRes.rows[0].id;

        // 2. Get equipment ID
        const equipRes = await client.query(
          `SELECT e.id, m.name as make, e.type 
           FROM equipments e
           JOIN make m ON m.id::text = e.make::text
           JOIN asset_type at ON at.id::text = e.type::text
           WHERE LOWER(e.model) = LOWER($1) AND LOWER(at.type) = LOWER($2)`,
          [model, type]
        );
        if (equipRes.rows.length === 0) {
          console.warn(
            `Sheet ${sheetName} Row ${i}: No equipment found for model ${model}`
          );
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
                specGenerator(type, mgmtIp, serviceIp, clusterIp, os)
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
          `Sheet ${sheetName} Row ${i}: Equipment ${name} mapped to rack ${rackName} successfully.`
        );

        // 5. Sleep 5 sec
        await sleep(5000);
      } catch (err) {
        console.error(`Sheet ${sheetName} Row ${i}: Error ->`, err.message);
      }
    }
  }
  await client.end();
}

// Example usage:
const sheetNames = [
  // "structure-dc-network",
  "structure-dc_system",
  // "structure-NBR-New-Bldg",
  // "structure-dch-network",
  "structure-dch_system",
  // "structure-dch",
  // "structure-mch",
  // "structure-mch_moduler",
  // "structure-dch-summary",
  // "structure-dch_moduler",
  // "structure-nbr-dr-network",
  // "structure-cch_moduler",
  // "structure-cch",
  // "structure-icd",
  // "structure-icd_moduler",
  // "structure-bch",
  // "structure-bch_moduler",
  // "structure-pch",
  // "structure-pch_moduler",
  // "structure-cchbond",
  // "structure-dhakabond",
  // "structure-adamjee",
  // "structure-UEPZ",
  // "structure-dhaka-epz",
  // "structure-cepz",
  // "structure-darshana",
  // "structure-bhomra",
  // "structure-banglabandha",
  // "structure-hilli",
  // "structure-burimari",
  // "structure-sonamasjid",
  // "structure-teknaf",
  // "structure-Akhawra",
  // "structure-rohanpur",
  // "structure-tamabil",
  // "structure-shonahut",
  // "structure-Shewla",
  // "structure-Dhanua",
  // "structure-bibirbazar",
];
processExcelSheets(sheetNames);
