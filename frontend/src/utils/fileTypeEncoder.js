const FILE_TYPE_MAP = {
  // Images
  png: 1,
  jpg: 2,
  jpeg: 3,
  gif: 4,
  bmp: 5,
  webp: 6,
  svg: 7,
  // Audio
  mp3: 10,
  wav: 11,
  ogg: 12,
  // Documents
  pdf: 20,
  txt: 21,
  md: 22,
  // Text (no file)
  "": 0,
};

const REVERSE_FILE_TYPE_MAP = Object.fromEntries(
  Object.entries(FILE_TYPE_MAP).map(([k, v]) => [v, k])
);

export const encodeValueWithType = (value, fileType = "") => {
  const typeCode = FILE_TYPE_MAP[fileType.toLowerCase()] || 0;
  return String.fromCharCode(typeCode) + value;
};

export const decodeValueWithType = (encodedValue) => {
  if (!encodedValue) return { type: "", value: "" };

  const typeCode = encodedValue.charCodeAt(0);
  const fileType = REVERSE_FILE_TYPE_MAP[typeCode] || "";
  const actualValue = encodedValue.slice(1);

  return { type: fileType, value: actualValue };
};

export const isPreviewableType = (fileType) => {
  const previewableTypes = [
    "png",
    "jpg",
    "jpeg",
    "gif",
    "bmp",
    "webp",
    "svg",
    "mp3",
    "wav",
    "ogg",
    "pdf",
    "txt",
    "md",
  ];
  return previewableTypes.includes(fileType.toLowerCase());
};

export const getFileTypeFromFile = (file) => {
  if (!file || !file.name) return "";
  const extension = file.name.split(".").pop()?.toLowerCase() || "";
  return extension;
};
