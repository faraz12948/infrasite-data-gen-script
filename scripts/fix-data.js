const { Client } = require("pg");

const client = new Client({
  user: "root",
  host: "localhost",
  database: "ca_mgt_v4",
  password: "ca-mgt",
  port: 5432,
});
async function fixExtraDeviceSpecification() {
  try {
    await client.connect();

    // Get all rows
    const res = await client.query(
      "SELECT id, extra_device_specification FROM equipment_rack_mapping"
    );

    for (const row of res.rows) {
      let parsedSpec = row.extra_device_specification;

      try {
        // Try parsing the JSON string
        parsedSpec = JSON.parse(row.extra_device_specification);
      } catch (err) {
        console.error(
          `❌ Invalid JSON for id=${row.id}:`,
          row.extra_device_specification
        );
        continue; // skip invalid JSON
      }

      // Update row with valid JSON (stringify to store as JSON/JSONB)
      await client.query(
        `UPDATE equipment_rack_mapping 
         SET extra_device_specification = $1 
         WHERE id = $2`,
        [parsedSpec, row.id]
      );

      console.log(`✅ Updated id=${row.id}`);
    }
  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.end();
  }
}

fixExtraDeviceSpecification();
