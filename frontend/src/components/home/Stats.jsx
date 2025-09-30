import { FiActivity } from "react-icons/fi";

export const Stats = ({ stats }) => {
  return (
    <div className="bg-sloth-brown rounded-xl p-6 border-4 border-sloth-brown-dark shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] text-sloth-yellow">
      <div className="flex items-center gap-2 mb-4">
        <FiActivity className="text-xl" />
        <h3 className="font-bold text-lg">Today's Pack Statistics</h3>
      </div>

      <div className="space-y-3">
        <div className="flex justify-between">
          <span>🔍 Successful Fetches:</span>
          <span className="font-bold">{stats.gets}</span>
        </div>
        <div className="flex justify-between">
          <span>💾 Records Saved:</span>
          <span className="font-bold">{stats.puts}</span>
        </div>
        <div className="flex justify-between">
          <span>🗑️ Records Deleted:</span>
          <span className="font-bold">{stats.deletes}</span>
        </div>
        <div className="flex justify-between">
          <span>📋 Scans Performed:</span>
          <span className="font-bold">{stats.scans}</span>
        </div>
        <div className="flex justify-between">
          <span>🔄 Iterators Created:</span>
          <span className="font-bold">{stats.iterates}</span>
        </div>
        <div className="flex justify-between">
          <span>❌ Errors:</span>
          <span className="font-bold">{stats.errors}</span>
        </div>
      </div>
    </div>
  );
};

export default Stats;
