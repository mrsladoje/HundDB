const RokicaRunning = "../../pics/rokica_running.png";
const Bone = "../../pics/bone.png";

export const DashboardSign = ({ dataLost }) => {
  return (
    <div className="flex justify-center items-start space-x-5 md:space-x-8 relative">
      <div className="flex-shrink-0">
        <img
          src={Bone}
          alt="Bone"
          className="hidden sm:block h-12 w-auto object-contain flex-shrink-0 rotate-[24deg] mt-2.5"
        />
      </div>
      <div className="text-center mb-8">
        <h1 className="text-3xl sm:text-4xl md:text-5xl font-bold text-sloth-brown-dark mb-4 tracking-wide">
          <i>Dash</i>board
        </h1>
        <p className="text-lg text-sloth-brown font-medium">
          Sit... Fetch... Query... Gooood database!
        </p>

        {dataLost && (
          <div className="mt-4 bg-red-100 border-4 border-red-500 rounded-xl p-4 shadow-[4px_4px_0px_0px_rgba(239,68,68,1)]">
            <div className="flex items-center justify-center gap-2 text-red-800">
              <FaDog className="text-4xl flex-shrink-0" />
              <span className="font-bold">
                Ruff news! Previous data was lost during loading. Starting
                fresh!
              </span>
            </div>
          </div>
        )}
      </div>
      <div className="flex-shrink-0">
        <img
          src={RokicaRunning}
          alt="Rokica the dog running"
          className="hidden sm:block h-20 w-auto object-contain flex-shrink-0"
        />
      </div>
    </div>
  );
};

export default DashboardSign;
