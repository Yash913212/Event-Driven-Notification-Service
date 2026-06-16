const { v4: uuidv4 } = require('uuid');
const kafkaProducer = require('../services/kafka.producer');
const dbService = require('../services/db.service');

// Publish user activity event
async function publishUserActivityEvent(req, res, next) {
  try {
    const { event_type, payload } = req.body;

    if (!event_type || typeof event_type !== 'string') {
      return res.status(400).json({ error: 'event_type is required and must be a string' });
    }

    const allowedEventTypes = ['user_liked_post', 'user_commented'];
    if (!allowedEventTypes.includes(event_type)) {
      return res.status(400).json({ error: `event_type must be one of: ${allowedEventTypes.join(', ')}` });
    }

    if (!payload || typeof payload !== 'object') {
      return res.status(400).json({ error: 'payload is required and must be an object' });
    }

    // Resolve actor
    const actorId = payload.user_id;
    if (!actorId || typeof actorId !== 'string') {
      return res.status(400).json({ error: 'payload.user_id is required and must be a string' });
    }

    // Resolve target (post_id or target_id)
    const targetId = payload.post_id || payload.target_id;
    if (!targetId || typeof targetId !== 'string') {
      return res.status(400).json({ error: 'payload.post_id or payload.target_id is required and must be a string' });
    }

    // Resolve recipient (recipient_id or liked_by_user_id or recipient_user_id)
    const recipientId = payload.recipient_id || payload.liked_by_user_id || payload.recipient_user_id;
    if (!recipientId || typeof recipientId !== 'string') {
      return res.status(400).json({ error: 'payload.recipient_id or payload.liked_by_user_id is required and must be a string' });
    }

    // Generate unique event details
    const eventId = uuidv4();
    const timestamp = new Date().toISOString();

    // Standardize event structure for Kafka
    const kafkaMessage = {
      event_id: eventId,
      timestamp,
      source: 'api-service',
      event_type,
      payload: {
        ...payload,
        user_id: actorId,
        target_id: targetId,
        recipient_id: recipientId
      }
    };

    // Publish to Kafka
    const topic = process.env.KAFKA_TOPIC || 'user-activity';
    await kafkaProducer.publishEvent(topic, kafkaMessage);

    return res.status(202).json({
      message: 'Event published successfully',
      event_id: eventId
    });
  } catch (error) {
    console.error('Error in publishUserActivityEvent:', error);
    return res.status(500).json({ error: 'Failed to publish event to message broker' });
  }
}

// Get user notifications
async function getUserNotifications(req, res, next) {
  try {
    const { userId } = req.params;
    if (!userId || typeof userId !== 'string' || userId.trim() === '') {
      return res.status(400).json({ error: 'userId parameter is required and must be a non-empty string' });
    }

    const notifications = await dbService.getUnreadNotifications(userId);
    return res.status(200).json(notifications);
  } catch (error) {
    console.error('Error in getUserNotifications:', error);
    return res.status(500).json({ error: 'Failed to retrieve notifications' });
  }
}

// Mark notification as read
async function markNotificationAsRead(req, res, next) {
  try {
    const { notificationId } = req.params;
    if (!notificationId || typeof notificationId !== 'string' || notificationId.trim() === '') {
      return res.status(400).json({ error: 'notificationId parameter is required and must be a non-empty string' });
    }

    const wasUpdated = await dbService.markNotificationAsRead(notificationId);
    if (!wasUpdated) {
      return res.status(404).json({ error: 'Notification not found' });
    }

    return res.status(204).send();
  } catch (error) {
    console.error('Error in markNotificationAsRead:', error);
    return res.status(500).json({ error: 'Failed to update notification status' });
  }
}

module.exports = {
  publishUserActivityEvent,
  getUserNotifications,
  markNotificationAsRead
};
