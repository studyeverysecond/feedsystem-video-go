package ratelimit

import (
	rediscache "feedsystem_video_go/internal/middleware/redis"
	jwt "feedsystem_video_go/internal/middleware/jwt"
	"fmt"
	"net/http"
	"strings"
	"time"
	"strconv"
	"github.com/gin-gonic/gin"
)
// Limit 是一个 Gin 中间件函数，用于限制请求的速率，防止恶意请求过多导致服务器过载。它使用 Redis 来跟踪每个用户或 IP 地址的请求次数，并在超过限制时返回 429 Too Many Requests 错误。
type KeyFunc func(*gin.Context) (string, bool)

func Limit(
	cache *rediscache.Client,
	keyPrefix string,
	maxRequests int64,
	window time.Duration,
	keyFunc KeyFunc,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		if cache == nil || keyFunc == nil || maxRequests <= 0 || window <= 0 {
			c.Next()
			return
		}
		// 获取请求的唯一标识（如 IP 地址或账户 ID）
		subject, ok := keyFunc(c)
		if !ok {
			c.Next()
			return
		}
		key := buildKey(keyPrefix, subject)
		count, err := cache.IncrementWithExpire(c.Request.Context(), key, window)
		if err != nil {
			c.Next()  
			return
		}
		if count > maxRequests {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "too many requests",
			})
			return
		}
		c.Next()
	}
}
// buildKey 构建 Redis 键，格式为 "feedsystem:ratelimit:{keyPrefix}:{subject}"，其中 keyPrefix 是一个可选的前缀，subject 是请求的唯一标识。
func buildKey(keyPrefix, subject string) string {
	keyPrefix = strings.TrimSpace(keyPrefix)
	if keyPrefix == "" {
		keyPrefix = "default"
	}
	return fmt.Sprintf("feedsystem:ratelimit:%s:%s", keyPrefix, strings.TrimSpace(subject))
}
// KeyByIP 是一个 KeyFunc 函数，用于根据客户端的 IP 地址生成唯一标识。如果无法获取 IP 地址，则返回 false。
func KeyByIP(c *gin.Context) (string, bool) {
	ip := strings.TrimSpace(c.ClientIP())
	if ip == "" {
		return "", false
	}
	return ip, true
}
// KeyByAccount 是一个 KeyFunc 函数，用于根据 JWT 中的账户 ID 生成唯一标识。如果无法获取账户 ID 或账户 ID 为 0，则返回 false。
func KeyByAccount(c *gin.Context) (string, bool) {
	accountID, err := jwt.GetAccountID(c)
	if err != nil || accountID == 0 {
		return "", false
	}
	return strconv.FormatUint(uint64(accountID), 10), true
}