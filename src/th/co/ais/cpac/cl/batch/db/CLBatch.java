package th.co.ais.cpac.cl.batch.db;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

import th.co.ais.cpac.cl.batch.Constants;
import th.co.ais.cpac.cl.batch.ConstantsDB;
import th.co.ais.cpac.cl.batch.db.CLOrder.CLBatcInfo;
import th.co.ais.cpac.cl.common.Context;
import th.co.ais.cpac.cl.common.UtilityLogger;
import th.co.ais.cpac.cl.template.database.DBConnectionPools;
import th.co.ais.cpac.cl.template.database.DBTemplatesExecuteQuery;
import th.co.ais.cpac.cl.template.database.DBTemplatesInsert;
import th.co.ais.cpac.cl.template.database.DBTemplatesResponse;
import th.co.ais.cpac.cl.template.database.DBTemplatesUpdate;

/**
 *
 * @author Sirirat
 */
public class CLBatch {

	protected final UtilityLogger logger;

	public CLBatch(UtilityLogger logger) {
		this.logger = logger;
	}

	public class ExecuteResponse extends DBTemplatesResponse<Boolean> {

		@Override
		protected Boolean createResponse() {
			return false;
		}

		@Override
		public void setResponse(Boolean boo) {
			response = boo;
		}
	}

	public class CLBatchInfo {

		protected CLBatchInfo() {
		}

		private BigDecimal batchId;
		private BigDecimal batchTypeId;
		private BigDecimal batchVersionNo;
		private Date batchStartDtm;
		private Date batchEndDtm;
		private String batchFileName;
		private String responseFileName;
		private ConstantsDB.OutboundStatus outboundStatus;
		private Date outboundStatusDtm;
		private ConstantsDB.InboundStatus inboundStatus;
		private Date inboundStatusDtm;
		private Date created;
		private String createdBy;
		private Date lastUpd;
		private String lastUpdBy;

		public BigDecimal getBatchId() {
			return batchId;
		}

		public void setBatchId(BigDecimal batchId) {
			this.batchId = batchId;
		}

		public BigDecimal getBatchTypeId() {
			return batchTypeId;
		}

		public void setBatchTypeId(BigDecimal batchTypeId) {
			this.batchTypeId = batchTypeId;
		}

		public BigDecimal getBatchVersionNo() {
			return batchVersionNo;
		}

		public void setBatchVersionNo(BigDecimal batchVersionNo) {
			this.batchVersionNo = batchVersionNo;
		}

		public Date getBatchStartDtm() {
			return batchStartDtm;
		}

		public void setBatchStartDtm(Date batchStartDtm) {
			this.batchStartDtm = batchStartDtm;
		}

		public Date getBatchEndDtm() {
			return batchEndDtm;
		}

		public void setBatchEndDtm(Date batchEndDtm) {
			this.batchEndDtm = batchEndDtm;
		}

		public String getBatchFileName() {
			return batchFileName;
		}

		public void setBatchFileName(String batchFileName) {
			this.batchFileName = batchFileName;
		}

		public String getResponseFileName() {
			return responseFileName;
		}

		public void setResponseFileName(String responseFileName) {
			this.responseFileName = responseFileName;
		}

		public Date getOutboundStatusDtm() {
			return outboundStatusDtm;
		}

		public void setOutboundStatusDtm(Date outboundStatusDtm) {
			this.outboundStatusDtm = outboundStatusDtm;
		}

		public Date getInboundStatusDtm() {
			return inboundStatusDtm;
		}

		public void setInboundStatusDtm(Date inboundStatusDtm) {
			this.inboundStatusDtm = inboundStatusDtm;
		}

		public Date getCreated() {
			return created;
		}

		public void setCreated(Date created) {
			this.created = created;
		}

		public String getCreatedBy() {
			return createdBy;
		}

		public void setCreatedBy(String createdBy) {
			this.createdBy = createdBy;
		}

		public Date getLastUpd() {
			return lastUpd;
		}

		public void setLastUpd(Date lastUpd) {
			this.lastUpd = lastUpd;
		}

		public String getLastUpdBy() {
			return lastUpdBy;
		}

		public void setLastUpdBy(String lastUpdBy) {
			this.lastUpdBy = lastUpdBy;
		}

		public ConstantsDB.OutboundStatus getOutboundStatus() {
			return outboundStatus;
		}

		public void setOutboundStatus(ConstantsDB.OutboundStatus outboundStatus) {
			this.outboundStatus = outboundStatus;
		}

		public ConstantsDB.InboundStatus getInboundStatus() {
			return inboundStatus;
		}

		public void setInboundStatus(ConstantsDB.InboundStatus inboundStatus) {
			this.inboundStatus = inboundStatus;
		}

	}

	public class CLBatchInfoResponse extends DBTemplatesResponse<ArrayList<CLBatchInfo>> {

		@Override
		protected ArrayList<CLBatchInfo> createResponse() {
			return new ArrayList<>();
		}

	}

	protected class FindBatchIDAction
			extends DBTemplatesExecuteQuery<CLBatchInfoResponse, UtilityLogger, DBConnectionPools> {
		private int inboundStatus;
		private String fileName;

		public FindBatchIDAction(UtilityLogger logger) {
			super(logger);
		}

		@Override
		protected CLBatchInfoResponse createResponse() {
			return new CLBatchInfoResponse();
		}

		@Override
		protected StringBuilder createSqlProcess() {
			StringBuilder sql = new StringBuilder();
			sql.append(" SELECT").append(ConstantsDB.END_LINE);
			sql.append(" BATCH_ID,BATCH_TYPE_ID,BATCH_VERSION_NO ").append(ConstantsDB.END_LINE);
			sql.append(" FROM dbo.CL_BATCH ").append(ConstantsDB.END_LINE);
			sql.append(" WHERE BATCH_FILE_NAME = ('").append(fileName).append("') ").append(ConstantsDB.END_LINE);
			sql.append(" and INBOUND_STATUS = (").append(inboundStatus).append(")").append(ConstantsDB.END_LINE);
			return sql;
		}

		@Override
		protected void setReturnValue(ResultSet resultSet) throws SQLException {
			CLBatchInfo temp = new CLBatchInfo();
			temp.setBatchId(resultSet.getBigDecimal("BATCH_ID"));
			temp.setBatchTypeId(resultSet.getBigDecimal("BATCH_TYPE_ID"));
			temp.setBatchVersionNo(resultSet.getBigDecimal("BATCH_VERSION_NO"));
			response.getResponse().add(temp);
		}

		protected CLBatchInfoResponse execute(int inboundStatus, String fileName) {
			this.inboundStatus = inboundStatus;
			this.fileName = fileName;
			return executeQuery(ConstantsDB.getDBConnectionPools(logger), true);
		}

	}

	public CLBatchInfo getBatchInfoByFileName(int inboundStatus, String fileName, Context context) throws Exception {
		CLBatchInfo batchInfo = null;

		CLBatchInfoResponse response = new FindBatchIDAction(logger).execute(inboundStatus, fileName);
		context.getLogger().debug("getBatchInfoByFileName->" + response.info().toString());

		switch (response.getStatusCode()) {
		case CLBatchInfoResponse.STATUS_COMPLETE: {
			batchInfo = response.getResponse().get(0);
			break;
		}
		case CLBatchInfoResponse.STATUS_DATA_NOT_FOUND: {
			break;
		}
		default: {
			throw new Exception("Error : " + response.getErrorMsg());
		}
		}

		return batchInfo;
	}

	protected class UpdateBatchReceiveAction
			extends DBTemplatesUpdate<ExecuteResponse, UtilityLogger, DBConnectionPools> {
		private int inboundStatus;
		private BigDecimal batchID;
		private String fileName;
		private String username;

		public UpdateBatchReceiveAction(UtilityLogger logger) {
			super(logger);
		}

		@Override
		protected ExecuteResponse createResponse() {
			return new ExecuteResponse();
		}

		@Override
		protected StringBuilder createSqlProcess() {
			StringBuilder sql = new StringBuilder();
			sql.append("UPDATE dbo.CL_BATCH ").append(ConstantsDB.END_LINE);
			sql.append("SET LAST_UPD= getdate() , LAST_UPD_BY='").append(username).append("'")
					.append(ConstantsDB.END_LINE);
			sql.append(",RESPONSE_FILE_NAME = '").append(fileName).append("'").append(ConstantsDB.END_LINE);
			sql.append(",INBOUND_STATUS = ").append(inboundStatus).append(ConstantsDB.END_LINE);
			sql.append(", INBOUND_STATUS_DTM = getdate() ").append(ConstantsDB.END_LINE);
			sql.append(" WHERE BATCH_ID = ").append(batchID).append(ConstantsDB.END_LINE);
			return sql;
		}

		protected ExecuteResponse execute(int inboundStatus, BigDecimal batchID, String fileName, String username) {
			this.inboundStatus = inboundStatus;
			this.batchID = batchID;
			this.fileName = fileName;
			this.username = username;
			return executeUpdate(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public ExecuteResponse updateInboundReceiveStatus(int inboundStatus, BigDecimal batchID, String fileName,
			String username, Context context) throws Exception {
		ExecuteResponse response = new UpdateBatchReceiveAction(logger).execute(inboundStatus, batchID, fileName,
				username);

		context.getLogger().debug("updateInboundReceiveStatus->" + response.info().toString());

		switch (response.getStatusCode()) {
		case CLBatchInfoResponse.STATUS_COMPLETE: {
			break;
		}
		case CLBatchInfoResponse.STATUS_DATA_NOT_FOUND: {
			break;
		}
		default: {
			throw new Exception("Error : " + response.getErrorMsg());
		}
		}

		return response;
	}

	protected class UpdateBatchCompleteAction
			extends DBTemplatesUpdate<ExecuteResponse, UtilityLogger, DBConnectionPools> {
		private BigDecimal batchID;
		private String username;

		public UpdateBatchCompleteAction(UtilityLogger logger) {
			super(logger);
		}

		@Override
		protected ExecuteResponse createResponse() {
			return new ExecuteResponse();
		}

		@Override
		protected StringBuilder createSqlProcess() {
			StringBuilder sql = new StringBuilder();
			sql.append("UPDATE dbo.CL_BATCH ").append(ConstantsDB.END_LINE);
			sql.append("SET LAST_UPD= getdate() , LAST_UPD_BY='").append(username).append("'")
					.append(ConstantsDB.END_LINE);
			sql.append(",INBOUND_STATUS = ").append(ConstantsDB.batchCompleteStatus).append(ConstantsDB.END_LINE);
			sql.append(", INBOUND_STATUS_DTM = getdate() ").append(ConstantsDB.END_LINE);
			sql.append(" WHERE BATCH_ID = ").append(batchID).append(ConstantsDB.END_LINE);
			return sql;
		}

		protected ExecuteResponse execute(BigDecimal batchID, String username) {
			this.batchID = batchID;
			this.username = username;
			return executeUpdate(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public ExecuteResponse updateInboundCompleteStatus(BigDecimal batchID, String username, Context context)
			throws Exception {

		ExecuteResponse response = new UpdateBatchCompleteAction(logger).execute(batchID, username);
		context.getLogger().debug("updateInboundCompleteStatus->" + response.info().toString());

		switch (response.getStatusCode()) {
		case CLBatchInfoResponse.STATUS_COMPLETE: {
			break;
		}
		case CLBatchInfoResponse.STATUS_DATA_NOT_FOUND: {
			break;
		}
		default: {
			throw new Exception("Error : " + response.getErrorMsg());
		}
		}

		return response;
	}

	public class CLBatchPathInfo {

		protected CLBatchPathInfo() {
		}

		private BigDecimal batchTypeId;
		private ConstantsDB.Environment environment;
		private String pathOutbound;
		private String pathInbound;

		public BigDecimal getBatchTypeId() {
			return batchTypeId;
		}

		public void setBatchTypeId(BigDecimal batchTypeId) {
			this.batchTypeId = batchTypeId;
		}

		public ConstantsDB.Environment getEnvironment() {
			return environment;
		}

		public void setEnvironment(ConstantsDB.Environment environment) {
			this.environment = environment;
		}

		public String getPathOutbound() {
			return pathOutbound;
		}

		public void setPathOutbound(String pathOutbound) {
			this.pathOutbound = pathOutbound;
		}

		public String getPathInbound() {
			return pathInbound;
		}

		public void setPathInbound(String pathInbound) {
			this.pathInbound = pathInbound;
		}

	}

	public class CLBatchPathResponse extends DBTemplatesResponse<CLBatchPathInfo> {

		@Override
		protected CLBatchPathInfo createResponse() {
			return new CLBatchPathInfo();
		}

	}

	protected class GetCLBatchPath
			extends DBTemplatesExecuteQuery<CLBatchPathResponse, UtilityLogger, DBConnectionPools> {

		public GetCLBatchPath(UtilityLogger logger) {
			super(logger);
		}

		@Override
		protected CLBatchPathResponse createResponse() {
			return new CLBatchPathResponse();
		}

		//
		@Override
		protected StringBuilder createSqlProcess() {
			StringBuilder sql = new StringBuilder();
			sql.append(" SELECT BATCH_TYPE_ID, ENVIRONMENT, PATH_OUTBOUND, PATH_INBOUND, RECORD_STATUS ");
			sql.append(" FROM dbo.CL_BATCH_PATH ");
			sql.append(" WHERE BATCH_TYPE_ID = ").append(batchTypeId.toPlainString()).append(" and RECORD_STATUS = 1 ");
			sql.append(" AND ENVIRONMENT = ").append(environment);
			return sql;
		}

		private int environment;
		private BigDecimal batchTypeId;

		@Override
		protected void setReturnValue(ResultSet resultSet) throws SQLException {
			CLBatchPathInfo temp = response.getResponse();
			temp.setBatchTypeId(resultSet.getBigDecimal("BATCH_TYPE_ID"));
			temp.setEnvironment(ConstantsDB.mapEnvironment(resultSet.getBigDecimal("ENVIRONMENT").intValue()));
			temp.setPathOutbound(resultSet.getString("PATH_OUTBOUND"));
			temp.setPathInbound(resultSet.getString("PATH_INBOUND"));
		}

		protected CLBatchPathResponse execute(BigDecimal batchTypeId, int environment) {
			this.batchTypeId = batchTypeId;
			this.environment = environment;
			return executeQuery(ConstantsDB.getDBConnectionPools(logger), true);
		}

	}

	public CLBatchPathResponse getCLBatchPath(BigDecimal batchTypeId, int environment) {
		return new GetCLBatchPath(logger).execute(batchTypeId, environment);
	}

	// <editor-fold defaultstate="collapsed" desc="Get CL_BATCH_VERSION Info">
	public class CLBatchVersionInfo {

		protected CLBatchVersionInfo() {
		}

		private BigDecimal batchTypeId;
		private BigDecimal batchVersionNo;
		private String batchNameFormat;
		private String batchFileType;
		private String batchEncoding;
		private String batchDelimiter;
		private BigDecimal limitPerFile;
		private BigDecimal limitPerDay;

		public BigDecimal getBatchTypeId() {
			return batchTypeId;
		}

		public void setBatchTypeId(BigDecimal batchTypeId) {
			this.batchTypeId = batchTypeId;
		}

		public BigDecimal getBatchVersionNo() {
			return batchVersionNo;
		}

		public void setBatchVersionNo(BigDecimal batchVersionNo) {
			this.batchVersionNo = batchVersionNo;
		}

		public String getBatchNameFormat() {
			return batchNameFormat;
		}

		public void setBatchNameFormat(String batchNameFormat) {
			this.batchNameFormat = batchNameFormat;
		}

		public String getBatchFileType() {
			return batchFileType;
		}

		public void setBatchFileType(String batchFileType) {
			this.batchFileType = batchFileType;
		}

		public String getBatchEncoding() {
			return batchEncoding;
		}

		public void setBatchEncoding(String batchEncoding) {
			this.batchEncoding = batchEncoding;
		}

		public String getBatchDelimiter() {
			return batchDelimiter;
		}

		public void setBatchDelimiter(String batchDelimiter) {
			this.batchDelimiter = batchDelimiter;
		}

		public BigDecimal getLimitPerFile() {
			return limitPerFile;
		}

		public void setLimitPerFile(BigDecimal limitPerFile) {
			this.limitPerFile = limitPerFile;
		}

		public BigDecimal getLimitPerDay() {
			return limitPerDay;
		}

		public void setLimitPerDay(BigDecimal limitPerDay) {
			this.limitPerDay = limitPerDay;
		}

	}

	public class GetCLBatchVersionResponse extends DBTemplatesResponse<CLBatchVersionInfo> {

		@Override
		protected CLBatchVersionInfo createResponse() {
			return new CLBatchVersionInfo();
		}
	}

	protected class GetCLBatchVersion
			extends DBTemplatesExecuteQuery<GetCLBatchVersionResponse, UtilityLogger, DBConnectionPools> {

		public GetCLBatchVersion(UtilityLogger logger) {
			super(logger);
		}

		@Override
		protected GetCLBatchVersionResponse createResponse() {
			return new GetCLBatchVersionResponse();
		}

		//
		@Override
		protected StringBuilder createSqlProcess() {
			StringBuilder sql = new StringBuilder();
			sql.append(Constants.END_LINE);
			sql.append(
					" SELECT BATCH_TYPE_ID, BATCH_VERSION_NO, BATCH_NAME_FORMAT, BATCH_FILE_TYPE, BATCH_ENCODING, BATCH_DELIMITER, LIMIT_PER_FILE,LIMIT_PER_DAY ")
					.append(Constants.END_LINE);
			sql.append(" FROM dbo.CL_BATCH_VERSION").append(Constants.END_LINE);
			sql.append(" WHERE BATCH_TYPE_ID = ").append(batchTypeId.toPlainString())
					.append(" and getdate() between EFFECT_START_DATE and isnull(EFFECT_END_DATE,getdate())")
					.append(Constants.END_LINE);
			;
			return sql;
		}

		private BigDecimal batchTypeId;

		@Override
		protected void setReturnValue(ResultSet resultSet) throws SQLException {
			CLBatchVersionInfo temp = response.getResponse();
			temp.setBatchTypeId(resultSet.getBigDecimal("BATCH_TYPE_ID"));
			temp.setBatchVersionNo(resultSet.getBigDecimal("BATCH_VERSION_NO"));
			temp.setBatchNameFormat(resultSet.getString("BATCH_NAME_FORMAT"));
			temp.setBatchFileType(resultSet.getString("BATCH_FILE_TYPE"));
			temp.setBatchEncoding(resultSet.getString("BATCH_ENCODING"));
			temp.setBatchDelimiter(resultSet.getString("BATCH_DELIMITER"));
			temp.setLimitPerFile(resultSet.getBigDecimal("LIMIT_PER_FILE"));
			temp.setLimitPerDay(resultSet.getBigDecimal("LIMIT_PER_DAY"));
		}

		protected GetCLBatchVersionResponse execute(BigDecimal batchTypeId) {
			this.batchTypeId = batchTypeId;
			return executeQuery(ConstantsDB.getDBConnectionPools(logger), true);
		}

	}

	public GetCLBatchVersionResponse getCLBatchVersion(BigDecimal batchTypeId) {
		return new GetCLBatchVersion(logger).execute(batchTypeId);
	}

	protected class InsertCLBatchProcess extends DBTemplatesInsert<ExecuteResponse, UtilityLogger, DBConnectionPools> {

		private CLBatchInfo batchInfo;

		public InsertCLBatchProcess(UtilityLogger logger) {
			super(logger);
		}

		@Override
		protected ExecuteResponse createResponse() {
			return new ExecuteResponse();
		}

		//
		@Override
		protected StringBuilder createSqlProcess() {
			StringBuilder column = new StringBuilder();
			StringBuilder value = new StringBuilder();

			genNumber("BATCH_TYPE_ID", batchInfo.getBatchTypeId(), null, column, value, false);
			genNumber("BATCH_VERSION_NO", batchInfo.getBatchVersionNo(), null, column, value, false);
			genDateTime("BATCH_START_DTM", batchInfo.getBatchStartDtm(), column, value, false);
			genDateTime("BATCH_END_DTM", batchInfo.getBatchEndDtm(), column, value, false);
			genString("BATCH_FILE_NAME", batchInfo.getBatchFileName(), column, value, false);
			genString("RESPONSE_FILE_NAME", batchInfo.getResponseFileName(), column, value, false);
			genNumber("OUTBOUND_STATUS", batchInfo.getOutboundStatus().getCode(), null, column, value, false);
			genDateTime("OUTBOUND_STATUS_DTM", batchInfo.getOutboundStatusDtm(), column, value, false);
			genNumber("INBOUND_STATUS", batchInfo.getInboundStatus().getCode(), null, column, value, false);
			genDateTime("INBOUND_STATUS_DTM", batchInfo.getInboundStatusDtm(), column, value, false);
			genMethod("CREATED", "getdate()", column, value, false);
			genString("CREATED_BY", batchInfo.getCreatedBy(), column, value, false);
			genMethod("LAST_UPD", "getdate()", column, value, false);
			genString("LAST_UPD_BY", batchInfo.getLastUpdBy(), column, value, false);

			StringBuilder sql = new StringBuilder();
			sql.append("INSERT INTO CL_BATCH(").append(column).append(")");
			sql.append("VALUES(").append(value).append(")");

			return sql;
		}


		protected ExecuteResponse execute(CLBatchInfo batchInfo) {
			this.batchInfo = batchInfo;
			return super.executeUpdateGetIdentity(ConstantsDB.getDBConnectionPools(logger), true);
		}

	}

	public CLBatchInfo buildCLBatchInfo() {
		return new CLBatchInfo();
	}
	public ExecuteResponse insertCLBatch(CLBatchInfo batchInfo) {
		return new InsertCLBatchProcess(logger).execute(batchInfo);
	}
}
