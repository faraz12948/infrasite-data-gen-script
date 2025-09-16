// syncAssociations.js
// Usage: set env vars PG_CONNECTION_STRING and BASE_URL then run: node syncAssociations.js

const XLSX = require("xlsx");
const path = require("path");
const { Client } = require("pg");
const axios = require("axios");

const EXCEL_PATH = path.join(__dirname, "NBR__logical-connectivity-data.xlsx");

// === Configure these (or set via env) ===
const pgConnectionString =
  process.env.PG_CONNECTION_STRING ||
  "postgresql://root:ca-mgt@localhost:5432/insert-institutions";
const baseUrl = process.env.BASE_URL || "http://localhost:5001";
const houseNameIdMap = {
  DR: "9745ec1d-7cc3-444a-b2f9-0196de9330ce",
  DC: "cf02adf7-0408-40a7-a4cb-0de18652dbc9",
  NDC: "ae612ba3-6f10-4308-921d-418c90bb96ff",
  CCH: "92550f0f-f3ae-4883-8199-8321702aa610",
  "NBR New Building": "3ae8c5d4-4803-44c2-87d3-3eda1cbf6fd0",
  DCH: "8191e270-5870-437e-9d5e-f165c6e37ec8",
  MCH: "20ee539d-ddbf-484a-b292-73df53ce1907",
  ICD: "ed4c2458-d8d1-41a4-a666-081d80dc44e0",
  BCH: "ebc70362-c841-41ad-88c4-c35dfa6a52c6",
  PCH: "79469b7b-2cea-4053-969e-007175a154ff",
  "CCH Bond": "11752b76-2a75-41fb-acf5-62dcb64c90dd",
  "Dhaka Bond": "8f475c68-08ff-41d8-ad96-fa466c8ce62b",
  Adamjee: "7beac6af-1d14-42fa-8396-7d692d548a75",
  "Uttara EPZ": "9a0f60a9-699c-47f8-9694-a7cf4f9831c9",
  "Dhaka EPZ": "e37277e1-cccd-472e-a858-52e0722f53d5",
  CEPZ: "c4220097-ba72-4135-a599-f0a04efe0bb6",
  Darshana: "4b7b47b5-d50e-41a7-b6aa-28ec37b084e4",
  Bhomra: "60bc421e-a538-461b-a7b5-ba6462cba1df",
  Banglabandha: "a48109ed-b0e0-44d7-b00b-6eff4d76b951",
  Hili: "dfa39f2e-c3e1-4653-b465-4bf8da92e4c6",
  Burimari: "3198c784-32d2-4ebf-901b-e2758d9c41ea",
  Sonamasjid: "1f49bca9-2891-4f1e-af08-f39fa90f3bcc",
  Teknaf: "b7266c14-7e3f-436a-8fcf-55abdfdbad67",
  Akhawra: "36c96ec0-4689-42cf-b23b-b8a7fb8194b0",
  Rohanpur: "e09f5906-0fe7-4fe6-95ee-6fdfab013fd8",
  Tamabil: "d22eb602-9d18-4de4-9014-f522e5f67285",
  Shonahat: "57b71ae8-ef34-4cb4-a5f5-08ff3bef6fa7",
  Shewla: "05eb7586-8bb7-4172-a5d5-433e30294115",
  Dhanua: "6ee7c4fd-f714-48d2-b77e-f6434f37fc5d",
  "Bibir Bazar": "3298f3b1-71c2-408e-8a7b-cb004d99bf16",
};

// === Postgres client ===
const pgClient = new Client({ connectionString: pgConnectionString });

// Helper: read Excel sheet into array of rows
function readSheet(sheet, workbook) {
  const worksheet = workbook.Sheets[sheet];
  if (!worksheet) return [];
  const rows = XLSX.utils.sheet_to_json(worksheet);
  // Trim all fields
  return rows.map((row) => {
    Object.keys(row).forEach(
      (k) => (row[k] = row[k] && row[k].toString().trim())
    );
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
  const url = `${baseUrl}/api/v1/cordinator/association/${encodeURIComponent(
    houseId
  )}`;
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

  let highNode = tree.children.find(
    (c) => c.name === "High Level Diagram" && c.type === "diagram"
  );
  if (!highNode) {
    // create default high level node
    highNode = {
      id: null, // server may generate id or accept null; we keep null to indicate new node (depends on API)
      name: "High Level Diagram",
      type: "diagram",
      summary: { list: [], type: "house", title: "", description: "" },
      parentId: tree.parentId || null,
      description: "",
      children: [],
    };
    tree.children.unshift(highNode); // add at beginning
  } else if (!highNode.children) {
    highNode.children = [];
  }

  return highNode;
}

// Check if an area with the same name already exists under a diagram node (by name)
function hasArea(diagramNode, areaName) {
  return (diagramNode.children || []).some(
    (c) => c.type === "area" && c.name === areaName
  );
}

// Helper: check if value is invalid (empty, N/A, null etc.)
function isInvalid(value) {
  if (!value) return true;
  const badValues = ["n/a", "na", "null", "-"];
  return badValues.includes(value.trim().toLowerCase());
}

// Build nested structure dynamically, skipping invalid levels + children
function buildNestedStructure(areaName, buildingName, floorName, roomName) {
  // Area is mandatory: if invalid -> return null
  if (isInvalid(areaName)) return null;

  const areaNode = {
    name: areaName,
    type: "area",
    summary: {
      list: [{ label: "", value: "" }],
      type: "",
      title: "",
      description: "",
    },
    description: "",
    children: [],
  };

  // Building
  if (!isInvalid(buildingName)) {
    const buildingNode = {
      name: buildingName,
      type: "building",
      summary: {
        list: [{ label: "", value: "" }],
        type: "",
        title: "",
        description: "",
      },
      description: "",
      children: [],
    };

    // Floor
    if (!isInvalid(floorName)) {
      const floorNode = {
        name: floorName,
        type: "floor",
        summary: {
          list: [{ label: "", value: "" }],
          type: "",
          title: "",
          description: "",
        },
        description: "",
        children: [],
      };

      // Room
      if (!isInvalid(roomName)) {
        const roomNode = {
          name: roomName,
          type: "room",
          summary: {
            list: [{ label: "", value: "" }],
            type: "",
            title: "",
            description: "",
          },
          description: "",
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
    console.log("Connecting to Postgres...");
    await pgClient.connect();

    const sheetNames = [
      "structure-NBR-New-Bldg",
      "structure-dc-network",
      "structure-dch-network",
      "structure-dch",
      "structure-mch",
      "structure-mch_moduler",
      "structure-dch-summary",
      "structure-dch_moduler",
      "structure-nbr-dr-network",
      "structure-cch_moduler",
      "structure-cch",
      "structure-icd_moduler",
      "structure-bch",
      "structure-bch_moduler",
      "structure-pch",
      "structure-pch_moduler",
      "structure-cchbond",
      "structure-dhakabond",
      "structure-adamjee",
      "structure-UEPZ",
      "structure-dhaka-epz",
      "structure-cepz",
      "structure-darshana",
      "structure-bhomra",
      "structure-banglabandha",
      "structure-hilli",
      "structure-burimari",
      "structure-sonamasjid",
      "structure-teknaf",
      "structure-Akhawra",
      "structure-rohanpur",
      "structure-tamabil",
      "structure-shonahut",
      "structure-Shewla",
      "structure-Dhanua",
      "structure-bibirbazar",
    ];
    const workbook = XLSX.readFile(EXCEL_PATH);

    for (const sheetName of sheetNames) {
      console.log(`Reading sheet: ${sheetName}`);
      const rows = readSheet(sheetName, workbook);
      console.log(`Found ${rows.length} rows in ${sheetName}.`);

      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        // expected columns: include, house, area, building, floor, room
        const include = (row.include || "").toLowerCase();
        if (include !== "yes") {
          console.log(
            `Sheet ${sheetName} Row ${i}: include != 'Yes' -> skipping`
          );
          continue;
        }

        const houseKey = row.house;
        if (!houseKey) {
          console.warn(
            `Sheet ${sheetName} Row ${i}: missing house value -> skipping`
          );
          continue;
        }

        const houseId = houseNameIdMap[houseKey];
        if (!houseId) {
          console.warn(
            `Sheet ${sheetName} Row ${i}: house "${houseKey}" not mapped in houseNameIdMap -> skipping`
          );
          continue;
        }

        const areaName = row.area;
        const buildingName = row.building;
        const floorName = row.floor;
        const roomName = row.room;

        if (!areaName || !buildingName || !floorName || !roomName) {
          console.warn(
            `Sheet ${sheetName} Row ${i}: missing one of area/building/floor/room -> skipping`
          );
          continue;
        }

        console.log(
          `Sheet ${sheetName} Row ${i}: processing house=${houseKey}(${houseId}), area="${areaName}"`
        );

        // Step 3-5: check psql if area already exists under parent = houseId
        let exists = false;
        try {
          exists = await areaExists(houseId, areaName);
        } catch (err) {
          console.error(
            `Sheet ${sheetName} Row ${i}: Postgres check failed:`,
            err.message
          );
          continue;
        }

        if (exists) {
          console.log(
            `Sheet ${sheetName} Row ${i}: area "${areaName}" found in DB under parent ${houseId} -> skipping insertion`
          );
          continue;
        }

        // Step 6: GET association tree for house
        let tree;
        try {
          tree = await getAssociationTree(houseId);
        } catch (err) {
          console.error(
            `Sheet ${sheetName} Row ${i}: GET association failed:`,
            err.message
          );
          continue;
        }

        // Step 7: find High Level Diagram block (or create it)
        const highNode = findOrCreateHighLevelDiagram(tree);

        // Step 8: insert area->building->floor->room under highNode
        if (hasArea(highNode, areaName)) {
          console.log(
            `Sheet ${sheetName} Row ${i}: area "${areaName}" already exists under High Level Diagram -> skipping add`
          );
          continue;
        }

        const areaNode = buildNestedStructure(
          areaName,
          buildingName,
          floorName,
          roomName
        );
        if (highNode.id) areaNode.parentId = highNode.id;

        highNode.children.push(areaNode);

        // Step 9: POST updated association tree
        const payload = {
          parentId: tree.parentId,
          children: tree.children,
        };

        try {
          const postResp = await postAssociationTree({
            parentId: payload.parentId,
            children: payload.children,
          });
          console.log(
            `Sheet ${sheetName} Row ${i}: POST result:`,
            postResp && postResp.messageCode ? postResp.messageCode : "OK"
          );
        } catch (err) {
          console.error(
            `Sheet ${sheetName} Row ${i}: POST failed:`,
            err.message || err
          );
        }

        await new Promise((r) => setTimeout(r, 200)); // 200ms
      }
    }

    console.log("Done processing Excel sheets.");
  } catch (err) {
    console.error("Fatal error:", err);
  } finally {
    try {
      await pgClient.end();
    } catch (e) {}
  }
})();
