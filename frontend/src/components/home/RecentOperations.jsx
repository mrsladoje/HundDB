import { FaDog } from "react-icons/fa";
export const RecentOperations = ({ operations, onOperationClick }) => {
  return (
    <div className="isolate relative bg-white rounded-xl px-6 md:px-[1.6rem] pb-6 border-4 border-sloth-brown shadow-[6px_6px_0px_0px_rgba(107,94,74,1)] max-h-[20rem] overflow-y-auto overflow-x-hidden scroll-bare">
      <h3 className="w-[104%] translate-x-[-1.5%] z-10 sticky top-0 bg-white pt-6 pb-4 pl-2 sm:pl-1.5 font-bold text-lg text-sloth-brown-dark flex items-center gap-2 border-b-2 border-sloth-brown">
        <FaDog />
        Recent Tail Wags
      </h3>

      <div className="space-y-2 pt-3">
        {operations.length === 0 ? (
          <p className="text-sloth-brown italic text-center py-1">
            No operations yet... ready to fetch some data?
          </p>
        ) : (
          operations.map((op) => (
            <div
              key={op.id}
              onClick={() => onOperationClick(op)}
              className={`p-3 rounded-lg border-2 cursor-pointer transition-all duration-300 ${
                op.success
                  ? "bg-green-50 active:bg-green-100/70 border-green-300 hover:border-green-400 hover:translate-x-1 md:hover:translate-x-0.5"
                  : op.notFoundMessage
                  ? "bg-yellow-50 active:bg-yellow-100/70 border-yellow-300 hover:border-yellow-400 hover:translate-x-1 md:hover:translate-x-0.5"
                  : "bg-red-50 active:bg-red-100/70 border-red-300 hover:border-red-400 hover:translate-x-1 md:hover:translate-x-0.5"
              }`}
            >
              <div className="flex justify-between items-start text-sm flex-wrap">
                <div className="flex items-start">
                  <span className="font-bold">{op.type}</span>
                  <span className="text-gray-600 ml-2 inline-block w-32 truncate text-left">
                    {op.key}
                  </span>
                </div>
                {op.type === "PREFIX_ITERATE" && (
                  <span
                    className={`ml-2 text-xs px-1 py-0.5 rounded ${
                      op.ended
                        ? "bg-gray-200 text-gray-600"
                        : "bg-blue-200 text-blue-700"
                    }`}
                  >
                    {op.ended ? "Ended" : "Active"}
                  </span>
                )}
                <span className="text-xs text-gray-500">{op.timestamp}</span>
              </div>
              <div
                className={`text-xs mt-1 ${
                  op.success
                    ? "text-green-600"
                    : op.notFoundMessage
                    ? "text-yellow-600"
                    : "text-red-600"
                }`}
              >
                {op.message || op.notFoundMessage}
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
};

export default RecentOperations;
