package th.co.ais.cpac.cl.batch.db;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import th.co.ais.cpac.cl.batch.ConstantsDB;
import th.co.ais.cpac.cl.batch.db.CLBatch.CLBatchInfoResponse;
import th.co.ais.cpac.cl.batch.db.CLOrder.CLOrderInfoResponse;
import th.co.ais.cpac.cl.batch.db.CLTmpActSiebel.CLTmpActSiebelResponse;
import th.co.ais.cpac.cl.batch.db.CLTmpActSiebel.ExecuteResponse;
import th.co.ais.cpac.cl.batch.db.CLTmpActSiebel.UpdateGenFileResultCompleteAction;
import th.co.ais.cpac.cl.batch.util.Utility;
import th.co.ais.cpac.cl.common.Context;
import th.co.ais.cpac.cl.common.UtilityLogger;
import th.co.ais.cpac.cl.template.database.DBConnectionPools;
import th.co.ais.cpac.cl.template.database.DBTemplatesExecuteQuery;
import th.co.ais.cpac.cl.template.database.DBTemplatesInsert;
import th.co.ais.cpac.cl.template.database.DBTemplatesResponse;
import th.co.ais.cpac.cl.template.database.DBTemplatesUpdate;

public class CLTmpActExempt {
	protected final UtilityLogger logger;

	public CLTmpActExempt(UtilityLogger logger) {
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

	public class CLTmpActExemptInfo {
		protected CLTmpActExemptInfo() {
		}

		private BigDecimal tmpId;
		private BigDecimal exemptCustomerId;
		private String baNo;
		private String mobileNo;
		private String mode;
		private String effectiveDate;
		private String expireDate;

		public BigDecimal getExemptCustomerId() {
			return exemptCustomerId;
		}

		public void setExemptCustomerId(BigDecimal exemptCustomerId) {
			this.exemptCustomerId = exemptCustomerId;
		}

		public String getMode() {
			return mode;
		}

		public void setMode(String mode) {
			this.mode = mode;
		}

		public String getEffectiveDate() {
			return effectiveDate;
		}

		public void setEffectiveDate(String effectiveDate) {
			this.effectiveDate = effectiveDate;
		}

		public String getExpireDate() {
			return expireDate;
		}

		public void setExpireDate(String expireDate) {
			this.expireDate = expireDate;
		}

		public BigDecimal getTmpId() {
			return tmpId;
		}

		public void setTmpId(BigDecimal tmpId) {
			this.tmpId = tmpId;
		}

		public String getBaNo() {
			return baNo;
		}

		public void setBaNo(String baNo) {
			this.baNo = baNo;
		}

		public String getMobileNo() {
			return mobileNo;
		}

		public void setMobileNo(String mobileNo) {
			this.mobileNo = mobileNo;
		}

	}

	protected class TruncateExempActLog extends DBTemplatesInsert<ExecuteResponse, UtilityLogger, DBConnectionPools> {

		public TruncateExempActLog(UtilityLogger logger) {
			super(logger);
		}

		@Override
		protected ExecuteResponse createResponse() {
			return new ExecuteResponse();
		}

		//
		@Override
		protected StringBuilder createSqlProcess() {
			StringBuilder sql = new StringBuilder();
			sql.append("Truncate Table CL_TMP_ACT_EXEMPT ");
			return sql;
		}

		protected ExecuteResponse execute() {
			return executeUpdate(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public ExecuteResponse truncateExempActLog(Context context) throws Exception {
		ExecuteResponse response = new TruncateExempActLog(logger).execute();
		context.getLogger().debug("truncateExempActLog->" + response.info().toString());

		switch (response.getStatusCode()) {
		case CLOrderInfoResponse.STATUS_COMPLETE: {
			break;
		}
		case CLOrderInfoResponse.STATUS_DATA_NOT_FOUND: {
			break;
		}
		default: {
			throw new Exception("Error : " + response.getErrorMsg());
		}
		}
		return response;
	}

	protected class InsertExempActLog extends DBTemplatesInsert<ExecuteResponse, UtilityLogger, DBConnectionPools> {
		private BigDecimal batchTypeId;

		public InsertExempActLog(UtilityLogger logger) {
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
			StringBuilder sql = new StringBuilder();

			sql.append(
					"INSERT INTO CL_TMP_ACT_EXEMPT( EXEMPT_CUSTOMER_ID, BA_NO, MOBILE_NO, MODE, EFFECTIVE_DATE, EXPIRED_DATE, WORK_ORDER_ID, GEN_FLAG, GEN_DATETIME) ").append(ConstantsDB.END_LINE);
			sql.append("SELECT E.EXEMPT_CUSTOMER_ID, E.BA_NO, E.MOBILE_NO,").append(ConstantsDB.END_LINE);
			sql.append("CASE WHEN E.ACTION_ID = 0 ").append(ConstantsDB.END_LINE);
			sql.append(
					"THEN (SELECT L.CONDITION_3 FROM CL_CFG_LOV L WHERE L.LOV_KEYWORD = 'ACTION_MODE' AND L.LOV_KEYVALUE = E.ACTION_MODE) ").append(ConstantsDB.END_LINE);
			sql.append("ELSE (SELECT A.ACTION_ABRV FROM CL_ACTION A WHERE A.ACTION_ID = E.ACTION_ID) ").append(ConstantsDB.END_LINE);
			sql.append("END AS MODE, ").append(ConstantsDB.END_LINE);
			sql.append("ISNULL(E.EXEMPT_APPRV_DTM, E.EXEMPT_START_DTM) AS EFFECTIVE_DATE, ").append(ConstantsDB.END_LINE);
			sql.append("ISNULL(E.EXEMPT_EXPIRE_DTM, E.EXEMPT_END_DTM) AS EXPIRED_DATE, ").append(ConstantsDB.END_LINE);
			sql.append("null,'N' ,null ").append(ConstantsDB.END_LINE);
			sql.append("FROM CL_EXEMPT T, CL_EXEMPT_CUSTOMER E ").append(ConstantsDB.END_LINE);
			sql.append("WHERE T.EXEMPT_ID = E.EXEMPT_ID ").append(ConstantsDB.END_LINE);
			sql.append("AND T.EXEMPT_STATUS = 1 ").append(ConstantsDB.END_LINE);
			sql.append("AND E.EXEMPT_STATUS = 1 ").append(ConstantsDB.END_LINE);
			sql.append(
					"AND CONVERT(varchar(8),ISNULL(E.EXEMPT_APPRV_DTM,E.CREATED),112)=CONVERT(varchar(8),DATEADD(day,-1,getdate()),112) ").append(ConstantsDB.END_LINE);
			sql.append("AND E.EXEMPT_LEVEL = 4 ").append(ConstantsDB.END_LINE);
			sql.append(
					"AND NOT EXISTS (SELECT * FROM CL_BATCH_EXEMPT BE, CL_BATCH B WHERE BE.BATCH_ID = B.BATCH_ID AND BE.EXEMPT_CUSTOMER_ID = E.EXEMPT_CUSTOMER_ID AND B.BATCH_TYPE_ID =")
					.append(batchTypeId).append(")").append(ConstantsDB.END_LINE);
			sql.append(" UNION ").append(ConstantsDB.END_LINE);
			sql.append("SELECT  ").append(ConstantsDB.END_LINE);
			sql.append("E.EXEMPT_CUSTOMER_ID, E.BA_NO, dbo.CL_F_GET_MOBILE_REF_BY_BA(E.BA_NO) AS MOBILE_NO, ").append(ConstantsDB.END_LINE);
			sql.append("CASE WHEN E.ACTION_ID = 0 ").append(ConstantsDB.END_LINE);
			sql.append(
					"THEN (SELECT L.CONDITION_3 FROM CL_CFG_LOV L WHERE L.LOV_KEYWORD = 'ACTION_MODE' AND L.LOV_KEYVALUE = E.ACTION_MODE) ").append(ConstantsDB.END_LINE);
			sql.append("ELSE (SELECT A.ACTION_ABRV FROM CL_ACTION A WHERE A.ACTION_ID = E.ACTION_ID) ").append(ConstantsDB.END_LINE);
			sql.append("END AS MODE, ").append(ConstantsDB.END_LINE);
			sql.append("ISNULL(E.EXEMPT_APPRV_DTM, E.EXEMPT_START_DTM) AS EFFECTIVE_DATE,  ").append(ConstantsDB.END_LINE);
			sql.append("ISNULL(E.EXEMPT_EXPIRE_DTM, E.EXEMPT_END_DTM) AS EXPIRED_DATE, ").append(ConstantsDB.END_LINE);
			sql.append("null,'N' ,null ").append(ConstantsDB.END_LINE);
			sql.append("FROM CL_EXEMPT T, CL_EXEMPT_CUSTOMER E ").append(ConstantsDB.END_LINE);
			sql.append("WHERE T.EXEMPT_ID = E.EXEMPT_ID ").append(ConstantsDB.END_LINE);
			sql.append("AND T.EXEMPT_STATUS = 1 ").append(ConstantsDB.END_LINE);
			sql.append("AND E.EXEMPT_STATUS = 1 ").append(ConstantsDB.END_LINE);
			sql.append(
					"AND CONVERT(varchar(8),ISNULL(E.EXEMPT_APPRV_DTM,E.CREATED),112)=CONVERT(varchar(8),DATEADD(day,-1,getdate()),112) ").append(ConstantsDB.END_LINE);
			sql.append("AND E.EXEMPT_LEVEL = 3 ").append(ConstantsDB.END_LINE);
			sql.append(
					"AND NOT EXISTS (SELECT * FROM CL_BATCH_EXEMPT BE, CL_BATCH B WHERE BE.BATCH_ID = B.BATCH_ID AND BE.EXEMPT_CUSTOMER_ID = E.EXEMPT_CUSTOMER_ID AND B.BATCH_TYPE_ID =")
					.append(batchTypeId).append(")").append(ConstantsDB.END_LINE);
			sql.append(" UNION ");
			sql.append(
					"SELECT E.EXEMPT_CUSTOMER_ID, B.BA_NO AS BA_NO, dbo.CL_F_GET_MOBILE_REF_BY_BA(B.BA_NO) AS MOBILE_NO,").append(ConstantsDB.END_LINE);
			sql.append("CASE WHEN E.ACTION_ID = 0 ").append(ConstantsDB.END_LINE);
			sql.append(
					"THEN (SELECT L.CONDITION_3 FROM CL_CFG_LOV L WHERE L.LOV_KEYWORD = 'ACTION_MODE' AND L.LOV_KEYVALUE = E.ACTION_MODE) ").append(ConstantsDB.END_LINE);
			sql.append("ELSE (SELECT A.ACTION_ABRV FROM CL_ACTION A WHERE A.ACTION_ID = E.ACTION_ID) END AS MODE,").append(ConstantsDB.END_LINE);
			sql.append("ISNULL(E.EXEMPT_APPRV_DTM, E.EXEMPT_START_DTM) AS EFFECTIVE_DATE,").append(ConstantsDB.END_LINE);
			sql.append("ISNULL(E.EXEMPT_EXPIRE_DTM, E.EXEMPT_END_DTM) AS EXPIRED_DATE,").append(ConstantsDB.END_LINE);
			sql.append("null,'N' ,null ").append(ConstantsDB.END_LINE);
			sql.append("FROM CL_EXEMPT T, CL_EXEMPT_CUSTOMER E ").append(ConstantsDB.END_LINE);
			sql.append("JOIN CL_BA_INFO B ON E.SA_NO = B.SA_NO ").append(ConstantsDB.END_LINE);
			sql.append("WHERE T.EXEMPT_ID = E.EXEMPT_ID ").append(ConstantsDB.END_LINE);
			sql.append("AND T.EXEMPT_STATUS = 1 ").append(ConstantsDB.END_LINE);
			sql.append("AND E.EXEMPT_STATUS = 1 ").append(ConstantsDB.END_LINE);
			sql.append(
					"AND CONVERT(varchar(8),ISNULL(E.EXEMPT_APPRV_DTM,E.CREATED),112)=CONVERT(varchar(8),DATEADD(day,-1,getdate()),112) ");
			sql.append("AND E.EXEMPT_LEVEL = 2 ").append(ConstantsDB.END_LINE);
			sql.append(
					"AND NOT EXISTS (SELECT * FROM CL_BATCH_EXEMPT BE, CL_BATCH B WHERE BE.BATCH_ID = B.BATCH_ID AND BE.EXEMPT_CUSTOMER_ID = E.EXEMPT_CUSTOMER_ID AND B.BATCH_TYPE_ID =")
					.append(batchTypeId).append(")").append(ConstantsDB.END_LINE);
			sql.append(" UNION ").append(ConstantsDB.END_LINE);
			sql.append(
					"SELECT E.EXEMPT_CUSTOMER_ID, B.BA_NO AS BA_NO, dbo.CL_F_GET_MOBILE_REF_BY_BA(B.BA_NO) AS MOBILE_NO,").append(ConstantsDB.END_LINE);
			sql.append("CASE WHEN E.ACTION_ID = 0 ").append(ConstantsDB.END_LINE);
			sql.append(
					"THEN (SELECT L.CONDITION_3 FROM CL_CFG_LOV L WHERE L.LOV_KEYWORD = 'ACTION_MODE' AND L.LOV_KEYVALUE = E.ACTION_MODE) ").append(ConstantsDB.END_LINE);
			sql.append("ELSE (SELECT A.ACTION_ABRV FROM CL_ACTION A WHERE A.ACTION_ID = E.ACTION_ID) END AS MODE,").append(ConstantsDB.END_LINE);
			sql.append("ISNULL(E.EXEMPT_APPRV_DTM, E.EXEMPT_START_DTM) AS EFFECTIVE_DATE,").append(ConstantsDB.END_LINE);
			sql.append("ISNULL(E.EXEMPT_EXPIRE_DTM, E.EXEMPT_END_DTM) AS EXPIRED_DATE,").append(ConstantsDB.END_LINE);
			sql.append("null,'N' ,null ").append(ConstantsDB.END_LINE);
			sql.append("FROM CL_EXEMPT T, CL_EXEMPT_CUSTOMER E ").append(ConstantsDB.END_LINE);
			sql.append("JOIN CL_BA_INFO B ON E.CA_NO = B.CA_NO ").append(ConstantsDB.END_LINE);
			sql.append("WHERE T.EXEMPT_ID = E.EXEMPT_ID ").append(ConstantsDB.END_LINE);
			sql.append("AND T.EXEMPT_STATUS = 1 ").append(ConstantsDB.END_LINE);
			sql.append("AND E.EXEMPT_STATUS = 1 ").append(ConstantsDB.END_LINE);
			sql.append(
					"AND CONVERT(varchar(8),ISNULL(E.EXEMPT_APPRV_DTM,E.CREATED),112)=CONVERT(varchar(8),DATEADD(day,-1,getdate()),112) ").append(ConstantsDB.END_LINE);
			sql.append("AND E.EXEMPT_LEVEL = 1 ").append(ConstantsDB.END_LINE);
			sql.append(
					"AND NOT EXISTS (SELECT * FROM CL_BATCH_EXEMPT BE, CL_BATCH B WHERE BE.BATCH_ID = B.BATCH_ID AND BE.EXEMPT_CUSTOMER_ID = E.EXEMPT_CUSTOMER_ID AND B.BATCH_TYPE_ID =")
					.append(batchTypeId).append(")").append(ConstantsDB.END_LINE);
			return sql;
		}

		protected ExecuteResponse execute(BigDecimal batchTypeId) {
			this.batchTypeId = batchTypeId;
			return executeUpdate(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public ExecuteResponse insertExempActLog(Context context, BigDecimal batchTypeId) throws Exception {
		ExecuteResponse response = new InsertExempActLog(logger).execute(batchTypeId);
		context.getLogger().debug("InsertExempActLog->" + response.info().toString());

		switch (response.getStatusCode()) {
		case CLOrderInfoResponse.STATUS_COMPLETE: {
			break;
		}
		case CLOrderInfoResponse.STATUS_DATA_NOT_FOUND: {
			break;
		}
		default: {
			throw new Exception("Error : " + response.getErrorMsg());
		}
		}
		return response;
	}

	public class CLTmpActExemptResponse extends DBTemplatesResponse<ArrayList<CLTmpActExemptInfo>> {

		@Override
		protected ArrayList<CLTmpActExemptInfo> createResponse() {
			return new ArrayList<>();
		}

	}

	protected class GetTmpActExemptInfoAction
			extends DBTemplatesExecuteQuery<CLTmpActExemptResponse, UtilityLogger, DBConnectionPools> {
		private BigDecimal maxRecord;

		public GetTmpActExemptInfoAction(UtilityLogger logger) {
			super(logger);
		}

		@Override
		protected CLTmpActExemptResponse createResponse() {
			return new CLTmpActExemptResponse();
		}

		@Override
		protected StringBuilder createSqlProcess() {
			StringBuilder sql = new StringBuilder();
			sql.append(" SELECT TOP ").append(maxRecord).append(ConstantsDB.END_LINE);
			sql.append(" TMP_ID,EXEMPT_CUSTOMER_ID,BA_NO,MOBILE_NO,MODE,EFFECTIVE_DATE,EXPIRED_DATE ")
					.append(ConstantsDB.END_LINE);
			sql.append(" FROM CL_TMP_ACT_EXEMPT ").append(ConstantsDB.END_LINE);
			sql.append(" WHERE GEN_FLAG = 'N'").append(ConstantsDB.END_LINE);
			return sql;
		}

		@Override
		protected void setReturnValue(ResultSet resultSet) throws SQLException {
			CLTmpActExemptInfo temp = new CLTmpActExemptInfo();
			temp.setTmpId(resultSet.getBigDecimal("TMP_ID"));
			temp.setExemptCustomerId(resultSet.getBigDecimal("EXEMPT_CUSTOMER_ID"));
			temp.setBaNo(resultSet.getString("BA_NO"));
			temp.setMobileNo(resultSet.getString("MOBILE_NO"));
			temp.setMode(resultSet.getString("MODE"));
			temp.setEffectiveDate(
					Utility.convertDateToString(resultSet.getTimestamp("EFFECTIVE_DATE"), "ddMMyyyy_HHmmss"));
			temp.setExpireDate(Utility.convertDateToString(resultSet.getTimestamp("EXPIRED_DATE"), "ddMMyyyy_HHmmss"));
			response.getResponse().add(temp);
		}

		protected CLTmpActExemptResponse execute(BigDecimal maxRecord) {
			this.maxRecord = maxRecord;
			return executeQuery(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public CLTmpActExemptResponse getTmpActExemptInfo(BigDecimal maxRecord, Context context) throws Exception {

		CLTmpActExemptResponse response = new GetTmpActExemptInfoAction(logger).execute(maxRecord);
		context.getLogger().debug("getTmpActExemptInfo->" + response.info().toString());

		switch (response.getStatusCode()) {
		case CLTmpActSiebelResponse.STATUS_COMPLETE: {
			break;
		}
		case CLTmpActSiebelResponse.STATUS_DATA_NOT_FOUND: {
			break;
		}
		default: {
			throw new Exception("Error : " + response.getErrorMsg());
		}
		}
		return response;
	}

	protected class UpdateGenFileResultCompleteAction
			extends DBTemplatesUpdate<ExecuteResponse, UtilityLogger, DBConnectionPools> {
		private BigDecimal maxRecord;

		public UpdateGenFileResultCompleteAction(UtilityLogger logger) {
			super(logger);
		}

		@Override
		protected ExecuteResponse createResponse() {
			return new ExecuteResponse();
		}

		@Override
		protected StringBuilder createSqlProcess() {
			StringBuilder sql = new StringBuilder();
			sql.append("UPDATE TOP ").append(maxRecord).append(ConstantsDB.END_LINE);
			sql.append(" CL_TMP_ACT_EXEMPT").append(ConstantsDB.END_LINE);
			sql.append(" SET GEN_FLAG='Y'").append(ConstantsDB.END_LINE);
			sql.append(" WHERE GEN_FLAG = 'N'").append(ConstantsDB.END_LINE);
			return sql;
		}

		protected ExecuteResponse execute(BigDecimal maxRecord) {
			this.maxRecord = maxRecord;
			return executeUpdate(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public ExecuteResponse updateGenFileResultComplete(BigDecimal maxRecord, Context context) throws Exception {

		ExecuteResponse response = new UpdateGenFileResultCompleteAction(logger).execute(maxRecord);
		context.getLogger().debug("updateGenFileResultComplete->" + response.info().toString());

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

	public class CLTmpActExemptCount {
		protected CLTmpActExemptCount() {
		}

		private int totalRecord;

		public int getTotalRecord() {
			return totalRecord;
		}

		public void setTotalRecord(int totalRecord) {
			this.totalRecord = totalRecord;
		}

	}
	public class CLTmpActExemptCountResponse extends DBTemplatesResponse<ArrayList<CLTmpActExemptCount>> {

		@Override
		protected ArrayList<CLTmpActExemptCount> createResponse() {
			return new ArrayList<>();
		}

	}

	protected class GetTmpActExemptCountAction
			extends DBTemplatesExecuteQuery<CLTmpActExemptCountResponse, UtilityLogger, DBConnectionPools> {

		public GetTmpActExemptCountAction(UtilityLogger logger) {
			super(logger);
		}

		@Override
		protected CLTmpActExemptCountResponse createResponse() {
			return new CLTmpActExemptCountResponse();
		}

		@Override
		protected StringBuilder createSqlProcess() {
			StringBuilder sql = new StringBuilder();
			sql.append(" SELECT COUNT(*) AS CNT ").append(ConstantsDB.END_LINE);
			sql.append(" FROM CL_TMP_ACT_EXEMPT ").append(ConstantsDB.END_LINE);
			sql.append(" WHERE GEN_FLAG = 'N'").append(ConstantsDB.END_LINE);
			return sql;
		}

		@Override
		protected void setReturnValue(ResultSet resultSet) throws SQLException {
			CLTmpActExemptCount temp = new CLTmpActExemptCount();
			temp.setTotalRecord(resultSet.getInt("CNT"));
			response.getResponse().add(temp);
		}

		protected CLTmpActExemptCountResponse execute() {
			return executeQuery(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public int getTmpActExemptCount(Context context) throws Exception {

		CLTmpActExemptCountResponse response = new GetTmpActExemptCountAction(logger).execute();
		context.getLogger().debug("getTmpActExemptCount->" + response.info().toString());

		switch (response.getStatusCode()) {
		case CLTmpActSiebelResponse.STATUS_COMPLETE: {
			break;
		}
		case CLTmpActSiebelResponse.STATUS_DATA_NOT_FOUND: {
			break;
		}
		default: {
			throw new Exception("Error : " + response.getErrorMsg());
		}
		}
		if(response!=null && response.getResponse()!=null &&response.getResponse().size()>0){
			return response.getResponse().get(0).getTotalRecord();
		}else{
			return 0;
		}
	}
}
