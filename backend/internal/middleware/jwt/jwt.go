package jwt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/auth"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"github.com/gin-gonic/gin"
)

// JWTAuth 是一个 Gin 中间件函数，用于验证请求中的 JWT 令牌，确保用户已认证，并将用户信息存储在请求上下文中供后续处理使用
func JWTAuth(accountRepo *account.AccountRepository, cache *rediscache.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing authorization header"})
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid authorization header"})
			return
		}

		tokenString := parts[1]

		claims, err := auth.ParseToken(tokenString)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired token"})
			return
		}
		check(c, claims, tokenString, accountRepo, cache)
	}
}
// SoftJWTAuth 是一个 Gin 中间件函数，类似于 JWTAuth，但它允许请求中没有 JWT 令牌，如果令牌存在则进行验证，否则继续处理请求而不设置用户信息
func SoftJWTAuth(accountRepo *account.AccountRepository, cache *rediscache.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		// 如果没有 Authorization 头，直接继续处理请求，不设置用户信息
		if authHeader == "" {
			c.Next()
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid authorization header"})
			return
		}

		tokenString := parts[1]

		claims, err := auth.ParseToken(tokenString)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired token"})
			return
		}

		check(c, claims, tokenString, accountRepo, cache)
	}
}

// check 验证 JWT 令牌的有效性，检查令牌是否被撤销，并将用户信息存储在请求上下文中供后续处理使用
func check(c *gin.Context, claims *auth.Claims, tokenString string, accountRepo *account.AccountRepository, cache *rediscache.Client) {
	key := fmt.Sprintf("account:%d", claims.AccountID)

	// 先查 Redis
	if cache != nil {
		cacheCtx, cancel := context.WithTimeout(c.Request.Context(), 50*time.Millisecond)
		defer cancel()

		b, err := cache.GetBytes(cacheCtx, key)
		if err == nil {
			if string(b) != tokenString {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token has been revoked"})
				return
			}
			c.Set("accountID", claims.AccountID)
			c.Set("username", claims.Username)
			c.Next()
			return
		}
	}

	// Redis 故障/未启用：查 DB 兜底
	accountInfo, err := accountRepo.FindByID(c.Request.Context(), claims.AccountID)
	if err != nil || accountInfo.Token == "" || accountInfo.Token != tokenString {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token has been revoked"})
		return
	}

	if cache != nil {
		cacheCtx, cancel := context.WithTimeout(c.Request.Context(), 50*time.Millisecond)
		defer cancel()

		if err := cache.SetBytes(cacheCtx, key, []byte(tokenString), 24*time.Hour); err != nil {
			log.Printf("failed to set cache: %v", err)
		}
	}

	c.Set("accountID", claims.AccountID)
	c.Set("username", claims.Username)
	c.Next()

}

func GetAccountID(c *gin.Context) (uint, error) {
	uidValue, exists := c.Get("accountID")
	if !exists {
		return 0, errors.New("accountID not found")
	}

	accountID, ok := uidValue.(uint)
	if !ok {
		return 0, errors.New("accountID has invalid type")
	}

	return accountID, nil
}

func GetUsername(c *gin.Context) (string, error) {
	val, exists := c.Get("username")
	if !exists {
		return "", errors.New("username not found")
	}

	username, ok := val.(string)
	if !ok {
		return "", errors.New("username has invalid type")
	}

	return username, nil
}
