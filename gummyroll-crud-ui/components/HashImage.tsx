import useHashImage from "../lib/hooks/useHashImage";
import Image, { ImageProps } from "next/image";

type Props = Readonly<Omit<{ data: string } & ImageProps, "layout" | "src">>;

export default function HashImage({ alt, data, ...rest }: Props) {
  const dataURL = useHashImage(data);
  return (
    <Image
      {...rest}
      alt={alt}
      height="100%"
      layout="responsive"
      src={dataURL}
      width="100%"
    />
  );
}
