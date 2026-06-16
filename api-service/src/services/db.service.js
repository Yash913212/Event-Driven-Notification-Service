const pool = require('../config/db.config');

async function getUnreadNotifications(userId) {
  try {
    const query = 'SELECT notification_id, recipient_user_id, event_type, message_content, status, created_at FROM notifications WHERE recipient_user_id = ? AND status = ? ORDER BY created_at DESC';
    const [rows] = await pool.query(query, [userId, 'unread']);
    return rows;
  } catch (error) {
    console.error(`Error fetching notifications for user ${userId}:`, error);
    throw error;
  }
}

async function markNotificationAsRead(notificationId) {
  try {
    const query = 'UPDATE notifications SET status = ? WHERE notification_id = ?';
    const [result] = await pool.query(query, ['read', notificationId]);
    return result.affectedRows > 0;
  } catch (error) {
    console.error(`Error marking notification ${notificationId} as read:`, error);
    throw error;
  }
}

module.exports = {
  getUnreadNotifications,
  markNotificationAsRead
};
