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
		context.getLogger().debug("insertSMSOutBound->" + response.info().toString());

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
					"INSERT INTO CL_TMP_ACT_EXEMPT( EXEMPT_CUSTOMER_ID, BA_NO, MOBILE_NO, MODE, EFFECTIVE_DATE, EXPIRED_DATE, WORK_ORDER_ID, GEN_FLAG, GEN_DATETIME) ");
			sql.append("SELECT E.EXEMPT_CUSTOMER_ID, E.BA_NO, E.MOBILE_NO,");
			sql.append("CASE WHEN E.ACTION_ID = 0 ");
			sql.append(
					"THEN (SELECT L.CONDITION_3 FROM CL_CFG_LOV L WHERE L.LOV_KEYWORD = 'ACTION_MODE' AND L.LOV_KEYVALUE = E.ACTION_MODE) ");
			sql.append("ELSE (SELECT A.ACTION_ABRV FROM CL_ACTION A WHERE A.ACTION_ID = E.ACTION_ID) ");
			sql.append("END AS MODE, ");
			sql.append("ISNULL(E.EXEMPT_APPRV_DTM, E.EXEMPT_START_DTM) AS EFFECTIVE_DATE, ");
			sql.append("ISNULL(E.EXEMPT_EXPIRE_DTM, E.EXEMPT_END_DTM) AS EXPIRED_DATE, ");
			sql.append("null,'N' ,null");
			sql.append("FROM CL_EXEMPT T, CL_EXEMPT_CUSTOMER E ");
			sql.append("WHERE T.EXEMPT_ID = E.EXEMPT_ID ");
			sql.append("AND T.EXEMPT_STATUS = 1 ");
			sql.append("AND E.EXEMPT_STATUS = 1 ");
			sql.append(
					"AND CONVERT(varchar(8),ISNULL(E.EXEMPT_APPRV_DTM,E.CREATED),112)=CONVERT(varchar(8),DATEADD(day,-1,getdate()),112) ");
			sql.append("AND E.EXEMPT_LEVEL = 4 ");
			sql.append("UNION ");
			sql.append("SELECT  ");
			sql.append("E.EXEMPT_CUSTOMER_ID, E.BA_NO, dbo.CL_F_GET_MOBILE_REF_BY_BA(E.BA_NO) AS MOBILE_NO, ");
			sql.append("CASE WHEN E.ACTION_ID = 0 ");
			sql.append(
					"THEN (SELECT L.CONDITION_3 FROM CL_CFG_LOV L WHERE L.LOV_KEYWORD = 'ACTION_MODE' AND L.LOV_KEYVALUE = E.ACTION_MODE) ");
			sql.append("ELSE (SELECT A.ACTION_ABRV FROM CL_ACTION A WHERE A.ACTION_ID = E.ACTION_ID) ");
			sql.append("END AS MODE, ");
			sql.append("ISNULL(E.EXEMPT_APPRV_DTM, E.EXEMPT_START_DTM) AS EFFECTIVE_DATE,  ");
			sql.append("ISNULL(E.EXEMPT_EXPIRE_DTM, E.EXEMPT_END_DTM) AS EXPIRED_DATE, ");
			sql.append("null,'N' ,null ");
			sql.append("FROM CL_EXEMPT T, CL_EXEMPT_CUSTOMER E ");
			sql.append("WHERE T.EXEMPT_ID = E.EXEMPT_ID ");
			sql.append("AND T.EXEMPT_STATUS = 1 ");
			sql.append("AND E.EXEMPT_STATUS = 1 ");
			sql.append(
					"AND CONVERT(varchar(8),ISNULL(E.EXEMPT_APPRV_DTM,E.CREATED),112)=CONVERT(varchar(8),DATEADD(day,-1,getdate()),112) ");
			sql.append("AND E.EXEMPT_LEVEL = 3 ");
			sql.append("UNION ");
			sql.append(
					"SELECT E.EXEMPT_CUSTOMER_ID, B.BA_NO AS BA_NO, dbo.CL_F_GET_MOBILE_REF_BY_BA(B.BA_NO) AS MOBILE_NO,");
			sql.append("CASE WHEN E.ACTION_ID = 0 ");
			sql.append(
					"THEN (SELECT L.CONDITION_3 FROM CL_CFG_LOV L WHERE L.LOV_KEYWORD = 'ACTION_MODE' AND L.LOV_KEYVALUE = E.ACTION_MODE) ");
			sql.append("ELSE (SELECT A.ACTION_ABRV FROM CL_ACTION A WHERE A.ACTION_ID = E.ACTION_ID) END AS MODE,");
			sql.append("ISNULL(E.EXEMPT_APPRV_DTM, E.EXEMPT_START_DTM) AS EFFECTIVE_DATE,");
			sql.append("ISNULL(E.EXEMPT_EXPIRE_DTM, E.EXEMPT_END_DTM) AS EXPIRED_DATE,");
			sql.append("null,'N' ,null ");
			sql.append("FROM CL_EXEMPT T, CL_EXEMPT_CUSTOMER E ");
			sql.append("JOIN CL_BA_INFO B ON E.SA_NO = B.SA_NO ");
			sql.append("WHERE T.EXEMPT_ID = E.EXEMPT_ID ");
			sql.append("AND T.EXEMPT_STATUS = 1 ");
			sql.append("AND E.EXEMPT_STATUS = 1 ");
			sql.append(
					"AND CONVERT(varchar(8),ISNULL(E.EXEMPT_APPRV_DTM,E.CREATED),112)=CONVERT(varchar(8),DATEADD(day,-1,getdate()),112) ");
			sql.append("AND E.EXEMPT_LEVEL = 2 ");
			sql.append("UNION ");
			sql.append(
					"SELECT E.EXEMPT_CUSTOMER_ID, B.BA_NO AS BA_NO, dbo.CL_F_GET_MOBILE_REF_BY_BA(B.BA_NO) AS MOBILE_NO,");
			sql.append("CASE WHEN E.ACTION_ID = 0 ");
			sql.append(
					"THEN (SELECT L.CONDITION_3 FROM CL_CFG_LOV L WHERE L.LOV_KEYWORD = 'ACTION_MODE' AND L.LOV_KEYVALUE = E.ACTION_MODE) ");
			sql.append("ELSE (SELECT A.ACTION_ABRV FROM CL_ACTION A WHERE A.ACTION_ID = E.ACTION_ID) END AS MODE,");
			sql.append("ISNULL(E.EXEMPT_APPRV_DTM, E.EXEMPT_START_DTM) AS EFFECTIVE_DATE,");
			sql.append("ISNULL(E.EXEMPT_EXPIRE_DTM, E.EXEMPT_END_DTM) AS EXPIRED_DATE,");
			sql.append("null,'N' ,null ");
			sql.append("FROM CL_EXEMPT T, CL_EXEMPT_CUSTOMER E ");
			sql.append("JOIN CL_BA_INFO B ON E.CA_NO = B.CA_NO ");
			sql.append("WHERE T.EXEMPT_ID = E.EXEMPT_ID ");
			sql.append("AND T.EXEMPT_STATUS = 1 ");
			sql.append("AND E.EXEMPT_STATUS = 1 ");
			sql.append(
					"AND CONVERT(varchar(8),ISNULL(E.EXEMPT_APPRV_DTM,E.CREATED),112)=CONVERT(varchar(8),DATEADD(day,-1,getdate()),112) ");
			sql.append("AND E.EXEMPT_LEVEL = 1 ");

			sql.append("INSERT INTO dbo.CL_TMP_ACT_SIEBEL ");
			sql.append(
					"SELECT B.CA_NO, S.BA_NO, S.MOBILE_NO, B.CATEGORY, B.SUBCATEGORY, S.ACTION_STATUS_DTM, T.TREATMENT_ID, ");
			sql.append("'SMS - Outbound','DEBT',null, 'N',null ");
			sql.append("FROM CL_SMS S ");
			sql.append("JOIN CL_MESSAGE M on S.MESSAGE_ID=M.MESSAGE_ID ");
			sql.append("JOIN CL_MESSAGE_TREATMENT MT  on  M.MESSAGE_ID=MT.MESSAGE_ID ");
			sql.append("JOIN CL_TREATMENT T on MT.TREATMENT_ID=T.TREATMENT_ID ");
			sql.append("JOIN CL_BA_INFO B on S.BA_NO=B.BA_NO ");
			sql.append("WHERE S.ACTION_STATUS=4  ");
			sql.append("AND ACTIVITY_LOG_BOO='N' ");
			sql.append(
					"AND CONVERT(varchar(8),S.ACTION_STATUS_DTM,112)=CONVERT(varchar(8),DATEADD(day,-1,getdate()),112) ");
			return sql;
		}

		protected ExecuteResponse execute() {
			return executeUpdate(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public ExecuteResponse insertExempActLog(Context context) throws Exception {
		ExecuteResponse response = new InsertExempActLog(logger).execute();
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
			temp.setEffectiveDate(Utility.convertDateToString(resultSet.getDate("EFFECTIVE_DATE"), "ddMMyyyy_hh24mmss"));
			temp.setExpireDate(Utility.convertDateToString(resultSet.getDate("EXPIRED_DATE"), "ddMMyyyy_hh24mmss"));
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

	public ExecuteResponse updateGenFileResultComplete(BigDecimal maxRecord, Context context)
			throws Exception {

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
}
