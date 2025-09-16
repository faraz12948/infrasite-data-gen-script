const XLSX = require("xlsx");
const { Client } = require("pg");
const axios = require("axios");

// DB client
const client = new Client({
  user: "root",
  host: "localhost",
  database: "ca_mgt_v2",
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

async function processExcelSheets(
  sheetNames,
  excelFilePath = "NBR__logical-connectivity-data.xlsx"
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
        const house = row.house;
        const rackName = row.rack.trim();
        const floorName = row.floor;
        const roomName = row.room;
        const rackUCount = row["rack-u-count"];
        const rackMake = row["rack-make"];
        const rackModel = row["rack-model"];

        if (!houseNameIdMap[house]) {
          console.warn(
            `Sheet ${sheetName} Row ${i}: Unknown house ${house}, skipping...`
          );
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
          headers: { "Content-Type": "application/json" },
        });

        console.log(
          `Sheet ${sheetName} Row ${i}: Rack ${rackName} inserted successfully.`
        );
      } catch (err) {
        console.error(`Sheet ${sheetName} Row ${i}: Error ->`, err.message);
      }
    }
  }
  await client.end();
}

// Example usage:
const sheetNames = [
  // "structure-NBR-New-Bldg",
  // "structure-dc-network",
  // "structure-dch-network",
  // "structure-dch",
  // "structure-mch",
  // "structure-mch_moduler",
  // "structure-dch-summary",
  // "structure-dch_moduler",
  // "structure-nbr-dr-network",
  // "structure-cch_moduler",
  // "structure-cch",
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
  "structure-rohanpur",
  "structure-tamabil",
  "structure-shonahut",
  "structure-Shewla",
  "structure-Dhanua",
  "structure-bibirbazar",
];
processExcelSheets(sheetNames);
