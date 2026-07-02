import { FormatMask } from './FormatMask';

const formatSerializedId = (serializedId) => {
  if (!serializedId) return '';
  const formatMask = new FormatMask();
  const number = serializedId.replace('@c.us', '');

  return formatMask.setPhoneFormatMask(number)?.replace('+55', '🇧🇷') || number;
};

export default formatSerializedId;
