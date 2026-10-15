package httpapi

// KYC-роуты: REST → gRPC kyc-service. Логики нет — только перевод
// форматов и права: клиентские операции доступны любому авторизованному,
// офицерские — только роли OFFICER (см. requireRole в middleware.go).

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	kycv1 "mini-core/services/kyc-service/gen/bank/kyc/v1"
)

// submitKyc — POST /kyc/applications: клиент подаёт заявку.
// user_id берём из проверенного токена (claims), а не из тела запроса:
// клиент подаёт заявку только от своего имени.
func (s *Server) submitKyc(c *gin.Context) {
	cl := claimsOf(c)
	resp, err := s.kyc.SubmitApplication(s.outgoing(c), &kycv1.SubmitApplicationRequest{
		UserId: cl.UserID,
	})
	if err != nil {
		s.grpcError(c, err)
		return
	}
	c.JSON(http.StatusCreated, resp)
}

// getKyc — GET /kyc/applications/:id.
func (s *Server) getKyc(c *gin.Context) {
	resp, err := s.kyc.GetApplication(s.outgoing(c), &kycv1.GetApplicationRequest{
		ApplicationId: c.Param("id"),
	})
	if err != nil {
		s.grpcError(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

// listKyc — GET /kyc/applications?status_filter=&page_size=&page_token=
// (для OFFICER: очередь заявок).
func (s *Server) listKyc(c *gin.Context) {
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	resp, err := s.kyc.ListApplications(s.outgoing(c), &kycv1.ListApplicationsRequest{
		PageSize:     int32(pageSize),
		PageToken:    c.Query("page_token"),
		StatusFilter: c.Query("status_filter"),
	})
	if err != nil {
		s.grpcError(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

// claimKyc — POST /kyc/applications/:id/claim (OFFICER берёт в работу).
func (s *Server) claimKyc(c *gin.Context) {
	cl := claimsOf(c)
	resp, err := s.kyc.ClaimApplication(s.outgoing(c), &kycv1.ClaimApplicationRequest{
		ApplicationId: c.Param("id"),
		OfficerId:     cl.UserID, // кто забрал — из токена, не из тела
	})
	if err != nil {
		s.grpcError(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

// approveKyc — POST /kyc/applications/:id/approve {comment} (OFFICER).
func (s *Server) approveKyc(c *gin.Context) {
	var in struct {
		Comment string `json:"comment"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		s.writeError(c, http.StatusBadRequest, "INVALID_ARGUMENT", "некорректный JSON")
		return
	}
	cl := claimsOf(c)
	resp, err := s.kyc.ApproveApplication(s.outgoing(c), &kycv1.ApproveApplicationRequest{
		ApplicationId: c.Param("id"),
		OfficerId:     cl.UserID,
		Comment:       in.Comment,
	})
	if err != nil {
		s.grpcError(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

// rejectKyc — POST /kyc/applications/:id/reject {reason} (OFFICER).
func (s *Server) rejectKyc(c *gin.Context) {
	var in struct {
		Reason string `json:"reason"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		s.writeError(c, http.StatusBadRequest, "INVALID_ARGUMENT", "некорректный JSON")
		return
	}
	cl := claimsOf(c)
	resp, err := s.kyc.RejectApplication(s.outgoing(c), &kycv1.RejectApplicationRequest{
		ApplicationId: c.Param("id"),
		OfficerId:     cl.UserID,
		Reason:        in.Reason,
	})
	if err != nil {
		s.grpcError(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

// requestDocsKyc — POST /kyc/applications/:id/request-docs {comment} (OFFICER).
func (s *Server) requestDocsKyc(c *gin.Context) {
	var in struct {
		Comment string `json:"comment"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		s.writeError(c, http.StatusBadRequest, "INVALID_ARGUMENT", "некорректный JSON")
		return
	}
	cl := claimsOf(c)
	resp, err := s.kyc.RequestMoreDocuments(s.outgoing(c), &kycv1.RequestMoreDocumentsRequest{
		ApplicationId: c.Param("id"),
		OfficerId:     cl.UserID,
		Comment:       in.Comment,
	})
	if err != nil {
		s.grpcError(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}
