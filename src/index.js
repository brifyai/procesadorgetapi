require('dotenv').config();

const { startProcessor } = require('./processor');

async function main() {
  const enabledRaw = String(process.env.PROCESSOR_ENABLED ?? process.env.GETAPI_WORKER_ENABLED ?? 'true').trim().toLowerCase();
  const enabled = !(enabledRaw === '0' || enabledRaw === 'false' || enabledRaw === 'no' || enabledRaw === 'off');

  if (!enabled) {
    console.log('Procesador deshabilitado por variable de entorno.');
    return;
  }

  await startProcessor();
}

main().catch((e) => {
  console.error('Fallo fatal del procesador:', e?.message || e);
  process.exitCode = 1;
});

