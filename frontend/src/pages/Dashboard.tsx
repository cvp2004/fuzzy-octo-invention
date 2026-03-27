import { useQuery } from '@tanstack/react-query';
import { useStats, useStatsHistory } from '../hooks/useStats';
import { useDisk } from '../hooks/useDisk';
import { useLogs } from '../hooks/useLogs';

import StatCard from '../components/StatCard';
import LevelDiagram from '../components/LevelDiagram';
import LogLine from '../components/LogLine';
import { useMem } from '../hooks/useMem';
import { LineChart, Line, XAxis, YAxis, ResponsiveContainer, Tooltip } from 'recharts';

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

export default function Dashboard() {
  const { data: stats } = useStats();
  const { data: history } = useStatsHistory();
  const { data: diskData } = useDisk();
  const { data: memData } = useMem();
  const { logs } = useLogs(10);
  const active = memData?.active;
  const immutableQ = memData?.immutable || [];
  const threshold = 10; // default dev threshold

  const samples = (history?.samples || []).slice(-60).map((s: any, i: number) => ({
    i,
    seq: s.seq,
    l0: s.l0_count,
    mem: s.mem_bytes,
  }));

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-mono font-bold text-white">Dashboard</h2>

      {/* Stat cards */}
      <div className="grid grid-cols-4 gap-4">
        <StatCard label="Key Count" value={stats?.key_count ?? '-'} />
        <StatCard label="Current Seq" value={stats?.seq ?? '-'} />
        <StatCard label="WAL Entries" value={stats?.wal_entry_count ?? '-'} />
        <StatCard
          label="L0 SSTables"
          value={stats?.l0_sstable_count ?? '-'}
          color={
            (stats?.l0_sstable_count ?? 0) >= 10
              ? 'text-accent-red'
              : (stats?.l0_sstable_count ?? 0) >= 7
              ? 'text-accent-amber'
              : 'text-accent-green'
          }
        />
      </div>

      {/* Middle row: 3 panels */}
      <div className="grid grid-cols-3 gap-4">
        {/* MemTable panel */}
        <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
          <div className="text-xs font-mono text-gray-500 uppercase tracking-wider mb-3">
            MemTable
          </div>
          {active && (
            <>
              <div className="text-sm font-mono text-gray-300 mb-2">
                Active: {active.entry_count} entries / {active.size_bytes} B
              </div>
              <div className="w-full bg-navy-700 rounded-full h-2 mb-3">
                <div
                  className="bg-accent-green h-2 rounded-full transition-all"
                  style={{
                    width: `${Math.min(100, (active.entry_count / threshold) * 100)}%`,
                  }}
                />
              </div>
              <div className="text-xs font-mono text-gray-500 mb-2">
                Immutable Queue ({immutableQ.length}/4)
              </div>
              <div className="flex gap-1">
                {[0, 1, 2, 3].map((i) => (
                  <div
                    key={i}
                    className={`h-4 flex-1 rounded ${
                      i < immutableQ.length
                        ? 'bg-accent-amber/40 border border-accent-amber/60'
                        : 'bg-navy-700 border border-navy-600'
                    }`}
                  />
                ))}
              </div>
            </>
          )}
        </div>

        {/* Level diagram */}
        <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
          <LevelDiagram
            diskData={diskData}
            compactionActive={stats?.compaction_active}
          />
        </div>

        {/* Throughput chart */}
        <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
          <div className="text-xs font-mono text-gray-500 uppercase tracking-wider mb-3">
            Seq Over Time
          </div>
          {samples.length > 1 ? (
            <ResponsiveContainer width="100%" height={120}>
              <LineChart data={samples}>
                <XAxis dataKey="i" hide />
                <YAxis hide />
                <Tooltip
                  contentStyle={{ background: '#1a2035', border: '1px solid #243049', fontSize: 11 }}
                  labelStyle={{ color: '#9ca3af' }}
                />
                <Line type="monotone" dataKey="seq" stroke="#22c55e" dot={false} strokeWidth={1.5} />
                <Line type="monotone" dataKey="l0" stroke="#3b82f6" dot={false} strokeWidth={1.5} />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div className="text-xs text-gray-600 font-mono h-[120px] flex items-center justify-center">
              Collecting samples...
            </div>
          )}
        </div>
      </div>

      {/* Live event feed */}
      <div className="bg-navy-800 border border-navy-600 rounded-lg">
        <div className="px-4 py-2 border-b border-navy-600">
          <span className="text-xs font-mono text-gray-500 uppercase tracking-wider">
            Live Events
          </span>
        </div>
        <div className="max-h-48 overflow-y-auto">
          {logs.length === 0 ? (
            <div className="px-4 py-3 text-xs font-mono text-gray-600">
              Waiting for events...
            </div>
          ) : (
            logs.map((entry, i) => <LogLine key={i} entry={entry} />)
          )}
        </div>
      </div>
    </div>
  );
}
