/**
 * Compile-time tests for `@erpc-cloud/config` typings of the generic
 * JSON-RPC passthrough architecture ("jsonrpc").
 *
 * Run via:
 *   pnpm --filter @erpc-cloud/config test
 */

import {
  createConfig,
  ArchitectureJsonRpc,
  UpstreamTypeJsonRpc,
  type NetworkArchitecture,
  type UpstreamType,
  type JsonRpcNetworkConfig,
} from "../index";

/* ─────────────────── 1. Realistic jsonrpc config compiles ─────────────── */

const _realisticJsonRpcConfig = createConfig({
  projects: [
    {
      id: "main",
      upstreams: [
        {
          id: "node-1",
          type: "jsonrpc",
          endpoint: "https://rpc.example.com",
          jsonRpc: { networkId: "bitcoin-mainnet" },
        },
        {
          // type is inferred as "jsonrpc" from jsonRpc.networkId
          id: "node-2",
          endpoint: "wss://rpc2.example.com",
          jsonRpc: { networkId: "bitcoin-mainnet", supportsBatch: false },
        },
      ],
      networks: [
        { architecture: "jsonrpc", jsonRpc: { id: "bitcoin-mainnet" } },
        // id defaults to the alias
        { architecture: "jsonrpc", alias: "stellar-mainnet" },
      ],
    },
  ],
});

/* ───────────────── 2. Root-level constants are values ─────────────────── */

const _architecture: NetworkArchitecture = ArchitectureJsonRpc as NetworkArchitecture;
const _upstreamType: UpstreamType = UpstreamTypeJsonRpc as UpstreamType;
const _network: JsonRpcNetworkConfig = { id: "bitcoin-mainnet" };

/* ───────────────────── 3. Negative cases (must error) ─────────────────── */

createConfig({
  projects: [
    {
      id: "bad-architecture",
      // @ts-expect-error — the architecture value is "jsonrpc", not "json-rpc"
      networks: [{ architecture: "json-rpc", jsonRpc: { id: "bitcoin-mainnet" } }],
    },
  ],
});

void _realisticJsonRpcConfig;
void _architecture;
void _upstreamType;
void _network;
