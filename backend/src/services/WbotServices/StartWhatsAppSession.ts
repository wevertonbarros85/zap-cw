import { initWASocket } from "../../libs/wbot";
import Whatsapp from "../../models/Whatsapp";
import { wbotMessageListener } from "./wbotMessageListener";
import { getIO } from "../../libs/socket";
import wbotMonitor from "./wbotMonitor";
import logger from "../../utils/logger";
import * as Sentry from "@sentry/node";
import { redisGroupCache } from "../../utils/RedisGroupCache";

export const StartWhatsAppSession = async (
  whatsapp: Whatsapp,
  companyId: number
): Promise<void> => {
  // ✅ CORREÇÃO: Verificar se whatsapp existe
  if (!whatsapp) {
    logger.error(`[StartWhatsAppSession] Whatsapp não fornecido para companyId ${companyId}`);
    return;
  }

  try {
    await whatsapp.update({ status: "OPENING" });
  } catch (updateErr) {
    logger.error(`[StartWhatsAppSession] Erro ao atualizar status: ${updateErr}`);
  }

  const io = getIO();
  io.of(String(companyId))
    .emit(`company-${companyId}-whatsappSession`, {
      action: "update",
      session: whatsapp
    });

  try {
    const wbot = await initWASocket(whatsapp);

    // ✅ CORREÇÃO: Verificar se wbot foi inicializado corretamente
    if (!wbot) {
      logger.error(`[StartWhatsAppSession] Falha ao inicializar wbot para whatsapp ${whatsapp.id}`);
      return;
    }

    if (wbot.id) {
      // ✅ CORREÇÃO: Tratar erro ao buscar grupos separadamente
      try {
        const groups = await wbot.groupFetchAllParticipating();
        if (groups && typeof groups === 'object') {
          for (const [id, groupMetadata] of Object.entries(groups)) {
            // Limpa os grupos existentes no cache
            await redisGroupCache.del(whatsapp.id, id);
            await redisGroupCache.set(whatsapp.id, id, groupMetadata);
          }
        }
      } catch (groupErr) {
        // ✅ CORREÇÃO: Não interromper sessão se falhar ao buscar grupos
        logger.warn(`[StartWhatsAppSession] Erro ao buscar grupos para whatsapp ${whatsapp.id}: ${groupErr}`);
      }

      wbotMessageListener(wbot, companyId);
      wbotMonitor(wbot, whatsapp, companyId);
    }
  } catch (err) {
    Sentry.captureException(err);
    logger.error(`[StartWhatsAppSession] Erro ao iniciar sessão whatsapp ${whatsapp.id}: ${err}`);
  }
};
