import { describe, it, expect, beforeEach, vi } from 'vitest';

// Importing main.ts pulls in RemoteTerminalView (extends the obsidian
// ItemView, which the unit-test obsidian mock doesn't model). Stub the leaf
// so the import graph resolves — the same shim openShadowVaultFor.test.ts uses.
vi.mock('../src/ui/RemoteTerminalView', () => ({
  RemoteTerminalView: class {},
  VIEW_TYPE_REMOTE_TERMINAL: 'remote-terminal',
}));

import { App, recordedNotices, clearNotices } from 'obsidian';
import RemoteSshPlugin from '../src/main';
import { DaemonUnavailableError } from '../src/ConnectionManager';
import { DaemonVerificationError } from '../src/transport/DaemonDownloader';
import { SyncState, type SshProfile } from '../src/types';
import { StatusBar } from '../src/ui/StatusBar';
import type { ReconnectState } from '../src/transport/ReconnectManager';

const rpcProfile = {
  id: 'p1', name: 'Prod', host: 'h', port: 22, username: 'u',
  remotePath: '~/v', transport: 'rpc', authMethod: 'agent',
} as unknown as SshProfile;

// Build a plugin whose ConnectionManager fails the RPC startup with `err`,
// with every other connectProfile collaborator stubbed to succeed. Exercises
// the REAL connectProfile control flow; only the transport/adapter
// collaborators are mocked. Returns the disconnect spy for teardown asserts.
function makePlugin(err: Error) {
  const plugin = new RemoteSshPlugin(new App() as never) as RemoteSshPlugin;
  const disconnect = vi.fn().mockResolvedValue(undefined);
  const conn = {
    isAlive: () => false,
    connectSsh: vi.fn().mockResolvedValue(undefined),
    startRpcSession: vi.fn().mockRejectedValue(err),
    rpcConnection: null as object | null,
    client: { disconnect },
  };
  const p = plugin as unknown as {
    settings: unknown;
    saveData: () => Promise<void>;
    conn: unknown;
    adapterMgr: unknown;
    statusBar: StatusBar;
    onReconnectStateChange: (state: ReconnectState) => void;
    setState: (state: SyncState) => void;
  };
  p.settings = { profiles: [rpcProfile], activeProfileId: null };
  p.saveData = vi.fn().mockResolvedValue(undefined);
  p.conn = conn;
  const statusEl = document.createElement('div');
  vi.spyOn(plugin, 'addStatusBarItem').mockReturnValue(statusEl);
  p.statusBar = new StatusBar(plugin, () => {});
  p.adapterMgr = { patch: vi.fn().mockResolvedValue(true), replayOfflineQueue: vi.fn().mockResolvedValue(undefined) };
  return { plugin, conn, disconnect, statusEl, p };
}

describe('connectProfile — RPC daemon failure handling (#399 / #406)', () => {
  beforeEach(() => clearNotices());

  it('downgrades to SFTP and still reaches CONNECTED when the daemon is unavailable', async () => {
    const { plugin, conn, disconnect, statusEl } = makePlugin(new DaemonUnavailableError('remote arch unsupported'));

    await plugin.connectProfile(rpcProfile);

    expect(conn.connectSsh).toHaveBeenCalledWith(rpcProfile);  // the SSH connect was really attempted
    expect(plugin.isConnected()).toBe(true);                   // connected, not errored out
    expect(disconnect).not.toHaveBeenCalled();                 // SSH session kept for SFTP
    expect(recordedNotices().some((n) => /SFTP/i.test(n))).toBe(true);
    expect(recordedNotices().some((n) => /conflict protection.*unavailable/i.test(n))).toBe(true);
    expect(statusEl.textContent).toMatch(/SFTP.*no conflict protection/i);
    expect(statusEl.classList.contains('is-warning')).toBe(true);
    expect(statusEl.classList.contains('is-connected')).toBe(false);
    expect(statusEl.title).toMatch(/overwrite/i);
  });

  it('downgrades to SFTP (not a hard fail) when the daemon deploy/startup fails, e.g. token timeout', async () => {
    // Field regression: a daemon that deploys but never writes its token
    // used to hard-fail the connect → populate skipped → EMPTY vault. A
    // generic (non-verification) daemon-start failure must degrade to SFTP.
    const { plugin, conn, disconnect } = makePlugin(
      new Error('ServerDeployer: daemon did not write /home/u/.obsidian-remote/token within 5000ms'),
    );

    await plugin.connectProfile(rpcProfile);

    expect(conn.connectSsh).toHaveBeenCalledWith(rpcProfile);
    expect(plugin.isConnected()).toBe(true);                   // reached CONNECTED → populate runs
    expect(disconnect).not.toHaveBeenCalled();                 // SSH kept for SFTP
    expect(recordedNotices().some((n) => /SFTP/i.test(n))).toBe(true);
    expect(recordedNotices().some((n) => /conflict protection.*unavailable/i.test(n))).toBe(true);
  });

  it('also warns when the profile explicitly selects plain SFTP', async () => {
    const { plugin, conn, statusEl } = makePlugin(new Error('RPC should not be started'));

    await plugin.connectProfile({ ...rpcProfile, transport: 'sftp' });

    expect(conn.startRpcSession).not.toHaveBeenCalled();
    expect(statusEl.textContent).toMatch(/SFTP.*no conflict protection/i);
    expect(recordedNotices().some((n) => /conflict protection.*unavailable/i.test(n))).toBe(true);
  });

  it('warns on reconnect downgrade and clears the warning when RPC recovers', () => {
    const { conn, statusEl, p } = makePlugin(new DaemonUnavailableError('daemon unavailable'));

    p.onReconnectStateChange({ kind: 'recovered' });

    expect(statusEl.textContent).toMatch(/SFTP.*no conflict protection/i);
    expect(statusEl.classList.contains('is-warning')).toBe(true);
    expect(recordedNotices().some((n) => /conflict protection.*unavailable/i.test(n))).toBe(true);

    clearNotices();
    conn.rpcConnection = {};
    p.onReconnectStateChange({ kind: 'recovered' });

    expect(statusEl.textContent).toMatch(/Connected/);
    expect(statusEl.classList.contains('is-warning')).toBe(false);
    expect(statusEl.title).not.toMatch(/overwrite/i);
    expect(recordedNotices().some((n) => /conflict protection.*unavailable/i.test(n))).toBe(false);
  });

  it('clears the persistent SFTP warning when the session disconnects', () => {
    const { statusEl, p } = makePlugin(new DaemonUnavailableError('daemon unavailable'));

    p.onReconnectStateChange({ kind: 'recovered' });
    expect(statusEl.classList.contains('is-warning')).toBe(true);
    p.setState(SyncState.IDLE);

    expect(statusEl.textContent).toMatch(/Disconnected/);
    expect(statusEl.classList.contains('is-warning')).toBe(false);
    expect(statusEl.title).not.toMatch(/overwrite/i);
  });

  it('fails LOUD (no silent SFTP downgrade) when daemon verification fails', async () => {
    const { plugin, conn, disconnect } = makePlugin(new DaemonVerificationError('sha256 mismatch'));

    await plugin.connectProfile(rpcProfile);

    expect(conn.connectSsh).toHaveBeenCalledWith(rpcProfile);  // got past connect, into RPC startup
    expect(plugin.isConnected()).toBe(false);                  // ERROR, not CONNECTED
    expect(disconnect).toHaveBeenCalledTimes(1);               // connection torn down
    // "loud" = a classified error Notice surfaced AND it did NOT take the
    // silent SFTP-downgrade path (that path emits a distinct "via SFTP" notice).
    expect(recordedNotices().length).toBeGreaterThan(0);
    expect(recordedNotices().some((n) => /SFTP/i.test(n))).toBe(false);
  });
});
