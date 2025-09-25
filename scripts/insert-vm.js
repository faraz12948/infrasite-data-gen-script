const XLSX = require("xlsx");
const path = require("path");
const axios = require("axios");
const { initLogger } = require("../logger");
const { Client } = require("pg");

initLogger("insert-vm");

const BASE_URL = "http://localhost:5001";
const pgConnectionString =
  process.env.PG_CONNECTION_STRING ||
  "postgresql://root:ca-mgt@localhost:5432/ca_mgt_v4";
const pgClient = new Client({ connectionString: pgConnectionString });

// Helper to read and parse CSV files
function readCsv(filePath) {
  try {
    const workbook = XLSX.readFile(filePath, { cellDates: true });
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    return XLSX.utils.sheet_to_json(worksheet);
  } catch (error) {
    console.error(`Error reading CSV file at ${filePath}:`, error.message);
    return [];
  }
}

// Main processing function
async function main() {
  try {
    await pgClient.connect();
    console.log("Connected to Postgres.");
    // 1. Read all data sources
    const vms = [
      ...readCsv(path.join(__dirname, "../csvs/EDC_VM_List.csv")),
      ...readCsv(path.join(__dirname, "../csvs/NDC_VM_List.csv")),
    ];

    const servers = [
      ...readCsv(path.join(__dirname, "../csvs/structure-dc_system.csv")),
      ...readCsv(path.join(__dirname, "../csvs/structure-dch_system.csv")),
    ];

    console.log(`Found ${vms.length} total VMs.`);
    console.log(`Found ${servers.length} total potential parent servers.`);

    // Create a map of servers by their tag for quick lookup
    const serverMap = new Map();
    for (const server of servers) {
      if (server.tag) {
        serverMap.set(String(server.tag).trim(), server);
      }
    }

    // 2. Iterate through VMs and create payloads
    for (const [i, vm] of vms.entries()) {
      const physicalServerTag = vm["Physical Server Tag"]?.toString().trim();
      const hostname = vm["Hostname/Tag"]?.toString().trim();

      // Skip if Hostname/Tag is empty
      if (!hostname) {
        console.warn(
          `VM #${i + 1} (${
            vm["OVM Name"]
          }): Skipping because 'Hostname/Tag' is empty.`
        );
        continue;
      }

      // Skip if Physical Server Tag is missing
      if (!physicalServerTag) {
        console.warn(
          `VM #${i + 1} (${
            vm["OVM Name"]
          }): Skipping because 'Physical Server Tag' is empty.`
        );
        continue;
      }

      // Find the parent server
      const server = serverMap.get(physicalServerTag);
      if (!server) {
        console.warn(
          `VM #${i + 1} (${
            vm["OVM Name"]
          }): No matching server found for tag '${physicalServerTag}'. Skipping.`
        );
        continue;
      }

      // Find rack and equipment details from the database
      const rackRes = await pgClient.query(
        `SELECT r.id as "rackId", erm.id as "equipmentRackMappingId", erm.equipment_id as "equipmentId"
         FROM racks r
         JOIN equipment_rack_mapping erm ON r.id = erm.parent_rack_id
         WHERE erm.tag = $1`,
        [physicalServerTag]
      );

      if (rackRes.rows.length === 0) {
        console.warn(
          `VM #${i + 1} (${
            vm["OVM Name"]
          }): Could not find rack/equipment mapping for server tag '${physicalServerTag}'. Skipping.`
        );
        continue;
      }
      const { rackId, equipmentId, equipmentRackMappingId } = rackRes.rows[0];

      // 3. Construct the payload
      const payload = {
        ovm_name: vm["OVM Name"],
        service_name: vm["Service Name"],
        ip: vm["Private IP"],
        hostname_tag: hostname,
        status: vm["Status"],
        spec: {
          rack_make: server["rack-make"],
          rack_rack: server["rack"],
          server_make: server["make"],
          rack_u_count: server["rack-u-count"]?.toString(),
          server_model: server["model"],
          rack_position: server["rack-position"]?.toString(),
          memory_gb_: vm["Memory (GB)"]?.toString(),
          processors: vm["Processors"]?.toString(),
          storage_gb_: vm["Storage\n(GB)"]?.toString(),
          operating_system: vm["Operating System"],
          storage_partition_gb_: vm["Storage-Partition\n(GB)"]?.toString(),
          server_series: "", // Keep empty as per instructions
        },
        rack_id: rackId,
        equipment_id: equipmentId,
        equipment_rack_mapping_id: equipmentRackMappingId,
      };

      // 4. Post to API
      try {
        console.log(`VM #${i + 1} (${vm["OVM Name"]}): Sending data to API...`);
        const response = await axios.post(`${BASE_URL}/api/v1/vm`, payload);
        if (response.data && response.data.messageCode === "SUCCESS") {
          console.log(
            `VM #${i + 1} (${
              vm["OVM Name"]
            }): Successfully inserted. ${JSON.stringify(response.data)}`
          );
        } else {
          console.error(
            `VM #${i + 1} (${vm["OVM Name"]}): API call failed with message: ${
              response.data.messageCode || "Unknown"
            }`
          );
        }
      } catch (error) {
        const errorMessage =
          error.response?.data?.message || error.message || "Unknown error";
        console.error(
          `VM #${i + 1} (${
            vm["OVM Name"]
          }): Error posting to API -> ${errorMessage}`
        );
      }

      // Delay to avoid overwhelming the server
      await new Promise((r) => setTimeout(r, 200));
    }

    console.log("Finished processing all VMs.");
  } catch (error) {
    console.error("A fatal error occurred in the main process:", error);
  } finally {
    await pgClient.end();
    console.log("Postgres connection closed.");
  }
}

main();
