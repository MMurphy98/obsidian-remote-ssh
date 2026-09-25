import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import * as fs from 'node:fs/promises';
import * as net from 'node:net';
import * as os from 'node:os';
import * as path from 'node:path';
import { establishRpcConnection, type RpcConnection } from '../../../src/transport/RpcConnection';
import { RpcRemoteFsClient } from '../../../src/adapter/RpcRemoteFsClient';

/** Real RPC framing/auth/handlers, without requiring SSH or Docker. All daemon
 * state (including its home-relative thumbnail cache) belongs to this fixture. */
export async function localRpcDaemon(): Promise<{
  root: string;
  vault: string;
  connection: RpcConnection;
  client: RpcRemoteFsClient;
  close(): Promise<void>;
}> {
  const binary = process.env.REMOTE_SSH_TEST_DAEMON ?? path.resolve(
    __dirname, '../../../server-bin', `obsidian-remote-server-${process.platform}-${process.arch === 'x64' ? 'amd64' : process.arch}`,
  );
  await fs.access(binary).catch(() => {
    throw new Error(`Missing test daemon: ${binary}. Run npm run build:server or set REMOTE_SSH_TEST_DAEMON.`);
  });
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'remote-ssh-replay-'));
  const vault = path.join(root, 'vault');
  const socketPath = path.join(root, 'rpc.sock');
  const tokenPath = path.join(root, 'token');
  await fs.mkdir(vault);
  const child = spawn(binary, ['--vault-root', vault, '--socket', socketPath, '--token-file', tokenPath], {
    env: { ...process.env, HOME: root },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  let stderr = '';
  let spawnError: Error | undefined;
  child.stderr?.on('data', (data: Buffer) => { stderr += data.toString(); });
  child.on('error', (error) => { spawnError = error; });
  let connection: RpcConnection | undefined;
  const close = async (): Promise<void> => {
    connection?.close();
    await stop(child);
    await fs.rm(root, { recursive: true, force: true });
  };
  try {
    const deadline = Date.now() + 10_000;
    for (;;) {
      if (spawnError) throw spawnError;
      if (child.exitCode !== null) throw new Error(`Test daemon exited: ${stderr}`);
      try {
        await fs.access(socketPath);
        break;
      } catch {
        if (Date.now() >= deadline) throw new Error(`Test daemon did not create its socket: ${stderr}`);
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
    }
    const stream = net.createConnection(socketPath);
    await once(stream, 'connect');
    connection = await establishRpcConnection({ stream, token: (await fs.readFile(tokenPath, 'utf8')).trim() });
    return { root, vault, connection, client: new RpcRemoteFsClient(connection.rpc), close };
  } catch (error) {
    await close();
    throw error;
  }
}

async function stop(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null || !child.pid) return;
  const exited = once(child, 'exit');
  child.kill('SIGTERM');
  const timeout = setTimeout(() => child.kill('SIGKILL'), 2_000);
  try { await exited; } finally { clearTimeout(timeout); }
}
