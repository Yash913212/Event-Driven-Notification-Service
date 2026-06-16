const request = require('supertest');
const app = require('../src/app');
const kafkaProducer = require('../src/services/kafka.producer');
const dbService = require('../src/services/db.service');

// Mock services to prevent actual DB/Kafka calls
jest.mock('../src/services/kafka.producer');
jest.mock('../src/services/db.service');
jest.mock('../src/config/db.config', () => ({
  query: jest.fn(),
  end: jest.fn()
}));
jest.mock('../src/config/kafka.config', () => ({
  producer: jest.fn(() => ({
    connect: jest.fn(),
    disconnect: jest.fn(),
    send: jest.fn()
  }))
}));

describe('API Service Unit Tests', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('POST /api/user-activity-events', () => {
    it('should return 400 if event_type is missing', async () => {
      const response = await request(app)
        .post('/api/user-activity-events')
        .send({
          payload: { user_id: 'user-1', post_id: 'post-1', recipient_id: 'user-2' }
        });
      expect(response.status).toBe(400);
      expect(response.body.error).toContain('event_type');
    });

    it('should return 400 if event_type is invalid', async () => {
      const response = await request(app)
        .post('/api/user-activity-events')
        .send({
          event_type: 'invalid_type',
          payload: { user_id: 'user-1', post_id: 'post-1', recipient_id: 'user-2' }
        });
      expect(response.status).toBe(400);
      expect(response.body.error).toContain('must be one of');
    });

    it('should return 400 if payload is missing', async () => {
      const response = await request(app)
        .post('/api/user-activity-events')
        .send({ event_type: 'user_liked_post' });
      expect(response.status).toBe(400);
      expect(response.body.error).toContain('payload is required');
    });

    it('should return 400 if payload.user_id is missing', async () => {
      const response = await request(app)
        .post('/api/user-activity-events')
        .send({
          event_type: 'user_liked_post',
          payload: { post_id: 'post-1', recipient_id: 'user-2' }
        });
      expect(response.status).toBe(400);
      expect(response.body.error).toContain('user_id');
    });

    it('should publish to Kafka and return 202 for valid request', async () => {
      kafkaProducer.publishEvent.mockResolvedValue({ success: true });

      const response = await request(app)
        .post('/api/user-activity-events')
        .send({
          event_type: 'user_liked_post',
          payload: { user_id: 'user-1', post_id: 'post-1', recipient_id: 'user-2' }
        });

      expect(response.status).toBe(202);
      expect(response.body.message).toBe('Event published successfully');
      expect(response.body.event_id).toBeDefined();
      expect(kafkaProducer.publishEvent).toHaveBeenCalled();
    });
  });

  describe('GET /api/users/:userId/notifications', () => {
    it('should return 200 and notifications array', async () => {
      const mockNotifications = [
        {
          notification_id: 'notif-1',
          recipient_user_id: 'user-2',
          event_type: 'user_liked_post',
          message_content: 'Your post was liked by user-1.',
          status: 'unread',
          created_at: new Date().toISOString()
        }
      ];
      dbService.getUnreadNotifications.mockResolvedValue(mockNotifications);

      const response = await request(app)
        .get('/api/users/user-2/notifications');

      expect(response.status).toBe(200);
      expect(response.body).toEqual(mockNotifications);
      expect(dbService.getUnreadNotifications).toHaveBeenCalledWith('user-2');
    });
  });

  describe('PATCH /api/notifications/:notificationId/read', () => {
    it('should return 204 if notification is marked as read', async () => {
      dbService.markNotificationAsRead.mockResolvedValue(true);

      const response = await request(app)
        .patch('/api/notifications/notif-1/read');

      expect(response.status).toBe(204);
      expect(dbService.markNotificationAsRead).toHaveBeenCalledWith('notif-1');
    });

    it('should return 404 if notification does not exist', async () => {
      dbService.markNotificationAsRead.mockResolvedValue(false);

      const response = await request(app)
        .patch('/api/notifications/notif-not-exist/read');

      expect(response.status).toBe(404);
      expect(response.body.error).toBe('Notification not found');
    });
  });
});
