import { FaBone } from "react-icons/fa";
import { FaHeart } from "react-icons/fa6";

export const BgDecorations = () => {
  return (
    <div className="absolute inset-0 opacity-10 overflow-hidden pointer-events-none">
      <FaBone className="absolute top-20 left-10 text-sloth-brown rotate-12 text-6xl" />
      <FaBone className="absolute top-40 right-20 text-sloth-brown -rotate-45 text-4xl" />
      <FaBone className="absolute bottom-32 left-1/4 text-sloth-brown rotate-75 text-5xl" />
      <FaBone className="absolute bottom-20 right-1/3 text-sloth-brown -rotate-12 text-3xl" />
      <FaHeart className="absolute top-60 left-1/2 text-sloth-brown rotate-12 text-4xl" />
    </div>
  );
};
