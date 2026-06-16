const notificationService = require('../src/services/notification.service');
const notificationModel = require('../src/models/notification.model');

// Mock database and Kafka connection configs
jest.mock('../src/models/notification.model');
jest.mock('../src/config/db.config', () => ({
  query: jest.fn(),
  end: jest.fn()
}));
jest.mock('../src/config/kafka.config', () => ({
  consumer: jest.fn(() => ({
    connect: jest.fn(),
    disconnect: jest.fn(),
    subscribe: jest.fn(),
    run: jest.fn()
  }))
}));

describe('Consumer Service Unit Tests', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('generateMessageContent', () => {
    it('should generate correct message for user_liked_post', () => {
      const msg = notificationService.generateMessageContent('user_liked_post', { user_id: 'alice' });
      expect(msg).toBe('Your post was liked by alice.');
    });

    it('should generate correct message for user_commented with comment text', () => {
      const msg = notificationService.generateMessageContent('user_commented', { user_id: 'bob', comment_text: 'Nice post!' });
      expect(msg).toBe('Your post was commented on by bob: "Nice post!"');
    });

    it('should generate correct message for user_commented without comment text', () => {
      const msg = notificationService.generateMessageContent('user_commented', { user_id: 'bob' });
      expect(msg).toBe('Your post was commented on by bob.');
    });

    it('should generate fallback message for unknown type', () => {
      const msg = notificationService.generateMessageContent('user_shared_post', { user_id: 'charlie' });
      expect(msg).toBe('New activity of type user_shared_post by charlie.');
    });
  });

  describe('processActivityEvent', () => {
    it('should return false and skip if event has already been processed (idempotency check 1)', async () => {
      notificationModel.isEventProcessed.mockResolvedValue(true);

      const event = {
        event_id: 'event-uuid-1',
        event_type: 'user_liked_post',
        payload: { user_id: 'user-1', recipient_id: 'user-2' }
      };

      const result = await notificationService.processActivityEvent(event);
      expect(result).toBe(false);
      expect(notificationModel.isEventProcessed).toHaveBeenCalledWith('event-uuid-1');
      expect(notificationModel.createNotification).not.toHaveBeenCalled();
    });

    it('should create notification and return true if event is new', async () => {
      notificationModel.isEventProcessed.mockResolvedValue(false);
      notificationModel.createNotification.mockResolvedValue(true);

      const event = {
        event_id: 'event-uuid-2',
        event_type: 'user_liked_post',
        payload: { user_id: 'user-1', recipient_id: 'user-2' }
      };

      const result = await notificationService.processActivityEvent(event);
      expect(result).toBe(true);
      expect(notificationModel.isEventProcessed).toHaveBeenCalledWith('event-uuid-2');
      expect(notificationModel.createNotification).toHaveBeenCalledWith(
        expect.objectContaining({
          recipientUserId: 'user-2',
          eventType: 'user_liked_post',
          processedEventId: 'event-uuid-2'
        })
      );
    });

    it('should return false if createNotification fails due to duplicate key (idempotency check 2)', async () => {
      notificationModel.isEventProcessed.mockResolvedValue(false);
      notificationModel.createNotification.mockResolvedValue(false);

      const event = {
        event_id: 'event-uuid-3',
        event_type: 'user_liked_post',
        payload: { user_id: 'user-1', recipient_id: 'user-2' }
      };

      const result = await notificationService.processActivityEvent(event);
      expect(result).toBe(false);
      expect(notificationModel.createNotification).toHaveBeenCalled();
    });
  });
});
