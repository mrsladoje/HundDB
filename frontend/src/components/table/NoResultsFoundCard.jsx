import { motion } from "framer-motion";
import { useEffect, useMemo, useRef, useState } from "react";
import { FaExclamationTriangle, FaRedo, FaSearch } from "react-icons/fa";
import { MdCancelPresentation } from "react-icons/md";

const NoResultsFoundCard = ({
  searchQuery = "",
  isPaginationError = false,
  onResetPagination,
  name = "items",
  theme = "green",
}) => {
  const [isVisible, setIsVisible] = useState(false);
  const cardRef = useRef(null);

  const themes = {
    green: {
      primary: "bg-green-500",
      secondary: "bg-green-100",
      accent: "text-green-600",
      accentLight: "text-green-400",
      accentDark: "text-green-800",
      border: "border-green-900",
      borderLight: "border-green-300",
      buttonBg: "bg-green-500",
      buttonHover: "hover:bg-green-600",
      buttonActive: "active:bg-green-600",
      lightBg: "bg-green-100",
      icon: "text-green-600",
      iconLight: "text-green-400",
      shadowColor: "rgb(34,197,94)", // green-500
      shadowColorHover: "rgba(22,163,74,1)", // green-600
    },
    yellow: {
      primary: "bg-yellow-500",
      secondary: "bg-yellow-100",
      accent: "text-yellow-600",
      accentLight: "text-yellow-400",
      accentDark: "text-yellow-800",
      border: "border-yellow-900",
      borderLight: "border-yellow-300",
      buttonBg: "bg-yellow-500",
      buttonHover: "hover:bg-yellow-600",
      buttonActive: "active:bg-yellow-600",
      lightBg: "bg-yellow-100",
      icon: "text-yellow-600",
      iconLight: "text-yellow-400",
      shadowColor: "rgb(234,179,8)", // yellow-500
      shadowColorHover: "rgba(202,138,4,1)", // yellow-600
    },
  };

  const themeColors = useMemo(() => themes[theme] || themes.green, [theme]);

  useEffect(() => {
    setIsVisible(true);
    return () => setIsVisible(false);
  }, []);

  // Character animation variants
  const characterVariants = {
    hidden: { opacity: 0, y: 10 },
    visible: (i) => ({
      opacity: 1,
      y: 0,
      transition: {
        delay: i * 0.02,
        duration: 0.15,
        ease: [0.22, 1, 0.36, 1],
      },
    }),
  };

  // Split text into array of characters with their index
  const splitText = (text, startIndex = 0) => {
    return Array.from(text).map((char, index) => (
      <motion.span
        key={`${startIndex}-${index}`}
        custom={startIndex + index}
        variants={characterVariants}
        initial="hidden"
        animate="visible"
        className="inline-block"
      >
        {char === " " ? "\u00A0" : char}
      </motion.span>
    ));
  };

  // Calculate total length of preceding text to maintain animation sequence
  const messagePrefix = isPaginationError
    ? "That page has run off chasing its tail for "
    : "There aren't any matches for ";
  const messagePrefixLength = messagePrefix.length;

  // Background icons for aesthetic
  const backgroundIcons = useMemo(
    () => [
      {
        Icon: FaSearch,
        size: "text-5xl",
        position: "top-10 left-10",
        rotate: "rotate-12",
      },
      {
        Icon: FaSearch,
        size: "text-6xl",
        position: "top-1/4 right-16",
        rotate: "rotate-12",
      },
      {
        Icon: FaExclamationTriangle,
        size: "text-5xl",
        position: "bottom-16 left-1/4",
        rotate: "rotate-12",
      },
      {
        Icon: FaSearch,
        size: "text-6xl",
        position: "top-1/3 left-16",
        rotate: "-rotate-12",
      },
      {
        Icon: FaExclamationTriangle,
        size: "text-5xl",
        position: "bottom-12 right-1/4",
        rotate: "rotate-6",
      },
      {
        Icon: FaSearch,
        size: "text-5xl",
        position: "top-20 right-1/3",
        rotate: "rotate-12",
      },
    ],
    []
  );

  // Memoize the animated search icons to prevent recalculations on rerenders
  const searchParticles = useMemo(() => {
    return Array(8)
      .fill(0)
      .map((_, i) => {
        // Pre-calculate random values once
        const initialX = Math.random() * 100 - 50;
        const initialY = Math.random() * 100 - 50;
        const targetX = Math.random() * 200 - 100;
        const targetY = Math.random() * 200 - 100;
        const topPosition = `${Math.random() * 100}%`;
        const leftPosition = `${Math.random() * 100}%`;
        const rotation = Math.random() * 360;
        const duration = 3 + Math.random() * 4;
        const delay = Math.random() * 2;
        const sizeClass = ["w-3 h-3", "w-4 h-4", "w-5 h-5"][
          Math.floor(Math.random() * 3)
        ];

        return {
          id: i,
          initialX,
          initialY,
          targetX,
          targetY,
          topPosition,
          leftPosition,
          rotation,
          duration,
          delay,
          sizeClass,
        };
      });
  }, []);

  // Generate inline styles for shadows to avoid JIT issues
  const cardShadowStyle = {
    boxShadow: `8px 8px 0px 0px ${themeColors.shadowColor}`,
  };

  const iconShadowStyle = {
    boxShadow: `4px 4px 0px 0px ${themeColors.shadowColor}`,
  };

  const buttonShadowStyle = {
    boxShadow: `4px 4px 0px 0px ${themeColors.shadowColorHover}`,
  };

  const buttonHoverShadowStyle = {
    boxShadow: `2px 2px 0px 0px ${themeColors.shadowColorHover}`,
  };

  const buttonActiveShadowStyle = {
    boxShadow: `0px 0px 0px 0px`,
  };

  return (
    <div
      className={`col-span-full relative h-80 overflow-hidden isolate rounded-xl border-2 ${themeColors.borderLight}`}
    >
      {/* Background styling with icons */}
      <div
        className={`absolute -inset-2 overflow-hidden z-10 ${themeColors.lightBg} opacity-90 blur-[0.9px]`}
      >
        {backgroundIcons.map((item, index) => (
          <div
            key={index}
            className={`absolute z-0 ${item.position} ${item.size} ${item.rotate} ${themeColors.accentLight} opacity-30`}
          >
            <item.Icon />
          </div>
        ))}
      </div>

      {/* Container for the card that centers it properly */}
      <div className="absolute inset-0 z-20 flex items-center justify-center">
        <motion.div
          ref={cardRef}
          initial={{ opacity: 0, scale: 0.9, y: 20 }}
          animate={{
            opacity: isVisible ? 1 : 0,
            scale: isVisible ? 1 : 0.9,
            y: isVisible ? 0 : 20,
          }}
          transition={{
            duration: 0.4,
            ease: [0.22, 1, 0.36, 1],
          }}
          className="w-full max-w-[29rem] pointer-events-auto px-4"
        >
          <div
            className={`relative bg-white border-4 ${themeColors.border} p-5`}
            style={cardShadowStyle}
          >
            {/* Corner accents */}
            <div
              className={`absolute -top-3 -left-3 w-5 h-5 ${themeColors.primary} border-4 ${themeColors.border}`}
            />
            <div
              className={`absolute -bottom-3 -right-3 w-5 h-5 ${themeColors.primary} border-4 ${themeColors.border}`}
            />

            {/* Top and bottom borders */}
            <div
              className={`absolute top-0 left-0 w-full h-2 ${themeColors.secondary}`}
            />
            <div
              className={`absolute bottom-0 right-0 w-full h-2 ${themeColors.secondary}`}
            />

            <div className="text-center space-y-2.5 relative z-10">
              {/* Animated icon */}
              <motion.div
                className="mx-auto"
                initial={{ scale: 0.5, opacity: 0 }}
                animate={{ scale: 1, opacity: 1 }}
                transition={{ duration: 0.5, delay: 0.1 }}
              >
                <div className="relative inline-block">
                  <motion.div
                    animate={{
                      rotate: [0, -5, 5, -5, 5, -3, 3, 0],
                      scale: [1, 1.03, 0.97, 1.03, 0.97, 1.01, 0.99, 1],
                    }}
                    transition={{
                      duration: 3.8,
                      repeat: Infinity,
                      repeatDelay: 2,
                    }}
                    className="relative z-10"
                  >
                    <div
                      className={`${themeColors.secondary} p-4 rounded-full border-4 ${themeColors.border}`}
                      style={iconShadowStyle}
                    >
                      {isPaginationError ? (
                        <MdCancelPresentation
                          className={`w-12 h-12 ${themeColors.icon}`}
                        />
                      ) : (
                        <FaSearch className={`w-12 h-12 ${themeColors.icon}`} />
                      )}
                    </div>
                  </motion.div>

                  <motion.div
                    initial={{ opacity: 0, scale: 0.8 }}
                    animate={{
                      opacity: [0, 0.7, 0],
                      scale: [0.8, 1.2, 1.5],
                      y: [0, -5, -10],
                    }}
                    transition={{
                      duration: 3,
                      repeat: Infinity,
                      repeatDelay: 2,
                    }}
                    className="absolute top-0 left-0 right-0 bottom-0 flex items-center justify-center"
                  >
                    {isPaginationError ? (
                      <FaExclamationTriangle
                        className={`w-16 h-16 ${themeColors.accentLight}`}
                      />
                    ) : (
                      <FaSearch
                        className={`w-16 h-16 ${themeColors.accentLight}`}
                      />
                    )}
                  </motion.div>
                </div>
              </motion.div>

              {/* Animated text */}
              <div>
                <h2
                  className={`text-xl md:text-2xl font-black ${themeColors.accentDark} mb-1 tracking-tight`}
                >
                  {splitText(
                    isPaginationError
                      ? "Ruff! Page Not Found"
                      : "No Results Found"
                  )}
                </h2>
                <p className={`${themeColors.accent} text-sm md:text-base`}>
                  {splitText(messagePrefix, 0)}
                  <span className="font-semibold">
                    {splitText(
                      `"${searchQuery}"`,
                      messagePrefixLength
                    )}
                  </span>
                </p>
              </div>

              {/* Button */}
              {isPaginationError && (
                <button
                  onClick={onResetPagination}
                  className={`inline-flex items-center gap-2 px-5 py-2 ${themeColors.buttonBg} ${themeColors.buttonHover} ${themeColors.buttonActive}
                  text-white font-bold tracking-wider text-sm rounded-md border-4 ${themeColors.border} 
                  transition-all duration-150 transform hover:translate-x-0.5 hover:translate-y-0.5
                  active:translate-x-1 active:translate-y-1 !active:shadow-none`}
                  style={buttonShadowStyle}
                  onMouseEnter={(e) => {
                    e.target.style.boxShadow = buttonHoverShadowStyle.boxShadow;
                  }}
                  onMouseLeave={(e) => {
                    e.target.style.boxShadow = buttonShadowStyle.boxShadow;
                  }}
                  onMouseDownCapture={(e) =>
                    (e.target.style.boxShadow =
                      buttonActiveShadowStyle.boxShadow)
                  }
                >
                  <FaRedo className="w-3.5 h-3.5" />
                  {"Back to Page 1"}
                </button>
              )}
            </div>

            {/* Animated particles */}
            <div className="absolute inset-0 overflow-hidden pointer-events-none">
              {searchParticles.map((particle) => (
                <motion.div
                  key={particle.id}
                  initial={{
                    x: particle.initialX,
                    y: particle.initialY,
                    opacity: 0,
                    scale: 0,
                  }}
                  animate={{
                    x: [particle.initialX, particle.targetX],
                    y: [particle.initialY, particle.targetY],
                    opacity: [0, 0.7, 0],
                    scale: [0, 1, 0],
                    rotate: [0, particle.rotation],
                  }}
                  transition={{
                    duration: particle.duration,
                    repeat: Infinity,
                    repeatDelay: particle.delay,
                    ease: "easeInOut",
                  }}
                  className={`absolute ${themeColors.iconLight} opacity-40`}
                  style={{
                    top: particle.topPosition,
                    left: particle.leftPosition,
                  }}
                >
                  <FaSearch className={particle.sizeClass} />
                </motion.div>
              ))}
            </div>
          </div>
        </motion.div>
      </div>
    </div>
  );
};

export default NoResultsFoundCard;
