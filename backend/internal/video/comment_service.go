package video

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"strings"

	"gorm.io/gorm"
)

type CommentService struct {
	repo            *CommentRepository
	VideoRepository *VideoRepository
	cache           *rediscache.Client
	commentMQ       *rabbitmq.CommentMQ
	popularityMQ    *rabbitmq.PopularityMQ
}

func NewCommentService(repo *CommentRepository, videoRepo *VideoRepository, cache *rediscache.Client, commentMQ *rabbitmq.CommentMQ, popularityMQ *rabbitmq.PopularityMQ) *CommentService {
	return &CommentService{repo: repo, VideoRepository: videoRepo, cache: cache, commentMQ: commentMQ, popularityMQ: popularityMQ}
}

func (s *CommentService) Publish(ctx context.Context, comment *Comment) error {
	if comment == nil {
		return errors.New("comment is nil")
	}
	comment.Username = strings.TrimSpace(comment.Username)
	comment.Content = strings.TrimSpace(comment.Content)
	if comment.VideoID == 0 || comment.AuthorID == 0 {
		return errors.New("video_id and author_id are required")
	}
	if comment.Content == "" {
		return errors.New("content is required")
	}

	//判断是否视频存在
	exists, err := s.VideoRepository.IsExist(ctx, comment.VideoID)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("video not found")
	}

	mysqlEnqueued := false
	redisEnqueued := false
	//先尝试将评论发布消息发送到消息队列，如果成功则直接返回成功响应，等待异步处理评论入库和视频热度更新。
	if s.commentMQ != nil {
		if err := s.commentMQ.Publish(ctx, comment.Username, comment.VideoID, comment.AuthorID, comment.Content); err == nil {
			mysqlEnqueued = true
		}
	}
	if s.popularityMQ != nil {
		if err := s.popularityMQ.Update(ctx, comment.VideoID, 1); err == nil {
			redisEnqueued = true
		}
	}
	if mysqlEnqueued && redisEnqueued {
		return nil
	}

	// Fallback: direct MySQL write when comment MQ publish fails.
	//如果消息队列存储失败，直接降级写入数据库，并更新视频的评论数和热度分。这里使用事务保证数据一致性。
	if !mysqlEnqueued {
		if err := s.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Select("id").First(&Video{}, comment.VideoID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return errors.New("video not found")
				}
				return err
			}
			if err := tx.Create(comment).Error; err != nil {
				return err
			}
			return tx.Model(&Video{}).Where("id = ?", comment.VideoID).
				UpdateColumn("popularity", gorm.Expr("popularity + 1")).Error
		}); err != nil {
			return err
		}
	}

	// Fallback: direct Redis update when popularity MQ publish fails.
	//如果消息队列存储失败，直接降级更新Redis中的视频热度分。
	if !redisEnqueued {
		UpdatePopularityCache(ctx, s.cache, comment.VideoID, 1)
	}
	return nil
}

func (s *CommentService) Delete(ctx context.Context, commentID uint, accountID uint) error {
	comment, err := s.repo.GetByID(ctx, commentID)
	if err != nil {
		return err
	}
	if comment == nil {
		return errors.New("comment not found")
	}
	if comment.AuthorID != accountID {
		return errors.New("permission denied")
	}
	if s.commentMQ != nil {
		if err := s.commentMQ.Delete(ctx, commentID); err == nil {
			return nil
		}
	}
	return s.repo.DeleteComment(ctx, comment)
}

//获取一条视频下的所有评论
func (s *CommentService) GetAll(ctx context.Context, videoID uint) ([]Comment, error) {
	exists, err := s.VideoRepository.IsExist(ctx, videoID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("video not found")
	}
	return s.repo.GetAllComments(ctx, videoID)
}
