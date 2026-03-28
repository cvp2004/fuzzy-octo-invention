import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  getEngineStatus,
  postEngineOpen,
  postEngineClose,
  postEngineReset,
} from '../api/client';

export default function EngineManager() {
  const [loading, setLoading] = useState<string | null>(null);
  const [confirmReset, setConfirmReset] = useState(false);
  const [message, setMessage] = useState<{ text: string; error: boolean } | null>(null);
  const qc = useQueryClient();

  const { data: status } = useQuery({
    queryKey: ['engine-status'],
    queryFn: getEngineStatus,
    refetchInterval: 2000,
  });

  const isOpen = status?.open ?? false;
  const uptime = status?.uptime_s ?? 0;
  const mins = Math.floor(uptime / 60);
  const secs = Math.floor(uptime % 60);

  const flash = (text: string, error = false) => {
    setMessage({ text, error });
    setTimeout(() => setMessage(null), 4000);
  };

  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ['engine-status'] });
    qc.invalidateQueries({ queryKey: ['stats'] });
    qc.invalidateQueries({ queryKey: ['stats-history'] });
    qc.invalidateQueries({ queryKey: ['disk'] });
    qc.invalidateQueries({ queryKey: ['mem'] });
  };

  const handleOpen = async () => {
    setLoading('open');
    try {
      await postEngineOpen();
      flash('Engine opened successfully.');
      invalidate();
    } catch (err: any) {
      flash(`Failed to open: ${err.message}`, true);
    } finally {
      setLoading(null);
    }
  };

  const handleClose = async () => {
    setLoading('close');
    try {
      await postEngineClose();
      flash('Engine closed.');
      invalidate();
    } catch (err: any) {
      flash(`Failed to close: ${err.message}`, true);
    } finally {
      setLoading(null);
    }
  };

  const handleReset = async () => {
    setLoading('reset');
    setConfirmReset(false);
    try {
      await postEngineReset();
      flash('Data deleted and engine restarted with fresh state.');
      invalidate();
    } catch (err: any) {
      flash(`Reset failed: ${err.message}`, true);
    } finally {
      setLoading(null);
    }
  };

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-mono font-bold text-white">Engine Manager</h2>

      {message && (
        <div
          className={`text-xs font-mono px-4 py-2 rounded border ${
            message.error
              ? 'bg-accent-red/10 border-accent-red/30 text-accent-red'
              : 'bg-accent-green/10 border-accent-green/30 text-accent-green'
          }`}
        >
          {message.text}
        </div>
      )}

      {/* Status card */}
      <div className="bg-navy-800 border border-navy-600 rounded-lg p-5 space-y-4">
        <h3 className="text-sm font-mono text-gray-400 uppercase tracking-wider">Status</h3>
        <div className="flex items-center gap-3">
          <span
            className={`w-3 h-3 rounded-full ${
              isOpen ? 'bg-accent-green animate-pulse' : 'bg-accent-red'
            }`}
          />
          <span className="font-mono text-lg text-white">
            {isOpen ? 'OPEN' : 'CLOSED'}
          </span>
        </div>
        {isOpen && (
          <div className="grid grid-cols-2 gap-4 text-sm font-mono">
            <div>
              <span className="text-gray-500">Data root</span>
              <div className="text-gray-200">{status?.data_root}</div>
            </div>
            <div>
              <span className="text-gray-500">Uptime</span>
              <div className="text-gray-200">{mins}m {secs}s</div>
            </div>
          </div>
        )}
      </div>

      {/* Controls */}
      <div className="bg-navy-800 border border-navy-600 rounded-lg p-5 space-y-4">
        <h3 className="text-sm font-mono text-gray-400 uppercase tracking-wider">Controls</h3>

        <div className="flex flex-wrap gap-3">
          <button
            onClick={handleOpen}
            disabled={isOpen || loading !== null}
            className="px-4 py-2 text-sm font-mono rounded border border-accent-green/40 text-accent-green bg-accent-green/10 hover:bg-accent-green/20 disabled:opacity-30 disabled:cursor-not-allowed"
          >
            {loading === 'open' ? 'Opening...' : 'Open Engine'}
          </button>

          <button
            onClick={handleClose}
            disabled={!isOpen || loading !== null}
            className="px-4 py-2 text-sm font-mono rounded border border-accent-yellow/40 text-accent-yellow bg-accent-yellow/10 hover:bg-accent-yellow/20 disabled:opacity-30 disabled:cursor-not-allowed"
          >
            {loading === 'close' ? 'Closing...' : 'Close Engine'}
          </button>
        </div>
      </div>

      {/* Danger zone */}
      <div className="bg-navy-800 border border-accent-red/30 rounded-lg p-5 space-y-4">
        <h3 className="text-sm font-mono text-accent-red uppercase tracking-wider">Danger Zone</h3>
        <p className="text-xs font-mono text-gray-400">
          This will close the engine, permanently delete all data (WAL, SSTables, config), and restart with a fresh empty state.
        </p>

        {!confirmReset ? (
          <button
            onClick={() => setConfirmReset(true)}
            disabled={loading !== null}
            className="px-4 py-2 text-sm font-mono rounded border border-accent-red/40 text-accent-red bg-accent-red/10 hover:bg-accent-red/20 disabled:opacity-30 disabled:cursor-not-allowed"
          >
            Delete Data &amp; Restart
          </button>
        ) : (
          <div className="flex items-center gap-3">
            <span className="text-xs font-mono text-accent-red">Are you sure?</span>
            <button
              onClick={handleReset}
              disabled={loading !== null}
              className="px-4 py-2 text-sm font-mono rounded bg-accent-red text-white hover:bg-accent-red/80"
            >
              {loading === 'reset' ? 'Resetting...' : 'Yes, delete everything'}
            </button>
            <button
              onClick={() => setConfirmReset(false)}
              className="px-4 py-2 text-sm font-mono rounded border border-navy-600 text-gray-400 hover:text-gray-200"
            >
              Cancel
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
