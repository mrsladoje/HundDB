export const NavButton = ({
  icon: Icon,
  label,
  isActive,
  onClick,
  className = "",
}) => {
  return (
    <button
      onClick={onClick}
      className={`
        group relative px-3 py-[0.575rem] font-semibold font-mono text-lg border-[3px] border-sloth-brown-dark
        transform transition-all duration-200 outline-none focus:ring-0
        ${
          isActive
            ? "bg-sloth-brown text-sloth-yellow shadow-inner translate-y-[4px] translate-x-[4px]"
            : "bg-sloth-yellow text-sloth-brown hover:bg-sloth-yellow-lite shadow-[4px_4px_0px_0px_#000000] active:translate-y-[4px] active:translate-x-[4px]"
        }
        ${className}
      `}
    >
      <div className="flex items-center gap-3">
        <Icon className="w-6 h-6" />
        <span className="hidden lg:block">{label}</span>
      </div>

      {/* Top border accent */}
      <div className="absolute -top-1.5 left-1/2 transform -translate-x-1/2 w-8 h-2 bg-sloth-brown border-l-2 border-r-2 border-t-2 border-black" />
    </button>
  );
};

export default NavButton;