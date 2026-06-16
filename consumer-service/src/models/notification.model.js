const dbPool = require('../config/db.config');

async function isEventProcessed(eventId) {
  try {
    const query = 'SELECT 1 FROM notifications WHERE processed_event_id = ? LIMIT 1';
    const [rows] = await dbPool.query(query, [eventId]);
    return rows.length > 0;
  } catch (error) {
    console.error(`Error checking if event ${eventId} is processed:`, error);
    throw error;
  }
}

async function createNotification(notification) {
  const { notificationId, recipientUserId, eventType, messageContent, processedEventId } = notification;
  try {
    const query = `
      INSERT INTO notifications (notification_id, recipient_user_id, event_type, message_content, processed_event_id)
      VALUES (?, ?, ?, ?, ?)
    `;
    await dbPool.query(query, [notificationId, recipientUserId, eventType, messageContent, processedEventId]);
    return true;
  } catch (error) {
    // Check for MySQL duplicate key error (code ER_DUP_ENTRY, errno 1062)
    if (error.code === 'ER_DUP_ENTRY' || error.errno === 1062) {
      console.warn(`Event ${processedEventId} has already been processed (detected via duplicate key constraint).`);
      return false;
    }
    console.error(`Error creating notification for event ${processedEventId}:`, error);
    throw error;
  }
}

module.exports = {
  isEventProcessed,
  createNotification
};
