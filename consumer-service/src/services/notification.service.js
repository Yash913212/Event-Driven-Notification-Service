const { v4: uuidv4 } = require('uuid');
const notificationModel = require('../models/notification.model');

function generateMessageContent(eventType, payload) {
  const actor = payload.user_id || 'Someone';
  
  switch (eventType) {
    case 'user_liked_post':
      return `Your post was liked by ${actor}.`;
    case 'user_commented':
      const commentText = payload.comment_text ? `: "${payload.comment_text}"` : '.';
      return `Your post was commented on by ${actor}${commentText}`;
    default:
      return `New activity of type ${eventType} by ${actor}.`;
  }
}

async function processActivityEvent(event) {
  const { event_id, event_type, payload } = event;

  if (!event_id) {
    console.error('Received event with missing event_id:', event);
    return false;
  }

  if (!event_type || !payload) {
    console.error(`Received invalid event structure for ID ${event_id}:`, event);
    return false;
  }

  const recipientUserId = payload.recipient_id;
  if (!recipientUserId) {
    console.error(`Missing recipient_id in payload for event ${event_id}`);
    return false;
  }

  // Idempotency Check 1: Check if already processed in database
  const alreadyProcessed = await notificationModel.isEventProcessed(event_id);
  if (alreadyProcessed) {
    console.log(`Event ${event_id} already processed. Skipping.`);
    return false;
  }

  // Generate content
  const messageContent = generateMessageContent(event_type, payload);
  const notificationId = uuidv4();

  const newNotification = {
    notificationId,
    recipientUserId,
    eventType: event_type,
    messageContent,
    processedEventId: event_id
  };

  // Idempotency Check 2: Try to insert (handles race conditions with DB unique constraint)
  const isCreated = await notificationModel.createNotification(newNotification);
  if (isCreated) {
    console.log(`Successfully processed event ${event_id}. Created notification ${notificationId}.`);
  }
  return isCreated;
}

module.exports = {
  processActivityEvent,
  generateMessageContent
};
