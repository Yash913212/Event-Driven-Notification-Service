const express = require('express');
const router = express.Router();
const notificationController = require('../controllers/notification.controller');

// Publish a user activity event to Kafka
router.post('/user-activity-events', notificationController.publishUserActivityEvent);

// Get all unread notifications for a specific user
router.get('/users/:userId/notifications', notificationController.getUserNotifications);

// Mark a specific notification as read
router.patch('/notifications/:notificationId/read', notificationController.markNotificationAsRead);

module.exports = router;
