package th.co.ais.cpac.cl.batch.db;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import th.co.ais.cpac.cl.batch.ConstantsDB;
import th.co.ais.cpac.cl.batch.db.CLBatch.CLBatchInfoResponse;
import th.co.ais.cpac.cl.batch.db.CLOrder.CLOrderInfoResponse;
import th.co.ais.cpac.cl.batch.util.Utility;
import th.co.ais.cpac.cl.common.Context;
import th.co.ais.cpac.cl.common.UtilityLogger;
import th.co.ais.cpac.cl.template.database.DBConnectionPools;
import th.co.ais.cpac.cl.template.database.DBTemplatesExecuteQuery;
import th.co.ais.cpac.cl.template.database.DBTemplatesInsert;
import th.co.ais.cpac.cl.template.database.DBTemplatesResponse;
import th.co.ais.cpac.cl.template.database.DBTemplatesUpdate;

public class CLTmpActSiebel {
	protected final UtilityLogger logger;

	public CLTmpActSiebel(UtilityLogger logger) {
		this.logger = logger;
	}

	public class CLTmpActSiebelInfo {
		protected CLTmpActSiebelInfo() {
		}

		private BigDecimal tmpId;
		private String baNo;
		private String caNo;
		private String mobileNo;
		private String category;
		private String subcateory;
		private String actionStatusDtm;
		private BigDecimal treatmentId;
		private String jobType;
		private String owner;

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

		public String getCaNo() {
			return caNo;
		}

		public void setCaNo(String caNo) {
			this.caNo = caNo;
		}

		public String getMobileNo() {
			return mobileNo;
		}

		public void setMobileNo(String mobileNo) {
			this.mobileNo = mobileNo;
		}

		public String getCategory() {
			return category;
		}

		public void setCategory(String category) {
			this.category = category;
		}

		public String getSubcateory() {
			return subcateory;
		}

		public void setSubcateory(String subcateory) {
			this.subcateory = subcateory;
		}

		public String getActionStatusDtm() {
			return actionStatusDtm;
		}

		public void setActionStatusDtm(String actionStatusDtm) {
			this.actionStatusDtm = actionStatusDtm;
		}

		public BigDecimal getTreatmentId() {
			return treatmentId;
		}

		public void setTreatmentId(BigDecimal treatmentId) {
			this.treatmentId = treatmentId;
		}

		public String getJobType() {
			return jobType;
		}

		public void setJobType(String jobType) {
			this.jobType = jobType;
		}

		public String getOwner() {
			return owner;
		}

		public void setOwner(String owner) {
			this.owner = owner;
		}
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

	protected class InsertSMSOutBound extends DBTemplatesInsert<ExecuteResponse, UtilityLogger, DBConnectionPools> {

		public InsertSMSOutBound(UtilityLogger logger) {
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

	public ExecuteResponse insertSMSOutBound(Context context) throws Exception {
		ExecuteResponse response = new InsertSMSOutBound(logger).execute();
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

	public class CLTmpActSiebelResponse extends DBTemplatesResponse<ArrayList<CLTmpActSiebelInfo>> {

		@Override
		protected ArrayList<CLTmpActSiebelInfo> createResponse() {
			return new ArrayList<>();
		}

	}

	protected class GetTmpactSiebelInfoAction
			extends DBTemplatesExecuteQuery<CLTmpActSiebelResponse, UtilityLogger, DBConnectionPools> {
		private String processName;
		private BigDecimal maxRecord;

		public GetTmpactSiebelInfoAction(UtilityLogger logger) {
			super(logger);
		}

		@Override
		protected CLTmpActSiebelResponse createResponse() {
			return new CLTmpActSiebelResponse();
		}

		@Override
		protected StringBuilder createSqlProcess() {
			StringBuilder sql = new StringBuilder();
			sql.append(" SELECT TOP ").append(maxRecord).append(ConstantsDB.END_LINE);
			sql.append(
					" TMP_ID,CA_NO,BA_NO,MOBILE_NO,CATEGORY,SUBCATEGORY,ACTION_STATUS_DTTM,TREATMENT_ID,JOB_TYPE,OWNER ")
					.append(ConstantsDB.END_LINE);
			sql.append(" FROM CL_TMP_ACT_SIEBEL ").append(ConstantsDB.END_LINE);
			sql.append(" WHERE JOB_TYPE = ('").append(processName).append("') ").append(ConstantsDB.END_LINE);
			sql.append(" and GEN_FLAG = 'N'").append(ConstantsDB.END_LINE);
			return sql;
		}

		@Override
		protected void setReturnValue(ResultSet resultSet) throws SQLException {
			CLTmpActSiebelInfo temp = new CLTmpActSiebelInfo();
			temp.setTmpId(resultSet.getBigDecimal("TMP_ID"));
			temp.setCaNo(resultSet.getString("CA_NO"));
			temp.setBaNo(resultSet.getString("BA_NO"));
			temp.setMobileNo(resultSet.getString("MOBILE_NO"));
			temp.setCategory(resultSet.getString("CATEGORY"));
			temp.setSubcateory(resultSet.getString("SUBCATEGORY"));
			temp.setActionStatusDtm(Utility.convertDateToString(resultSet.getDate("ACTION_STATUS_DTTM"), "ddMMyyyy_hh24mmss"));
			temp.setTreatmentId(resultSet.getBigDecimal("TREATMENT_ID"));
			temp.setJobType(resultSet.getString("JOB_TYPE"));
			temp.setOwner(resultSet.getString("OWNER"));
			response.getResponse().add(temp);
		}

		protected CLTmpActSiebelResponse execute(String processName, BigDecimal maxRecord) {
			this.processName = processName;
			this.maxRecord = maxRecord;
			return executeQuery(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public CLTmpActSiebelResponse getTmpActSiebelInfo(String processName, BigDecimal maxRecord, Context context)
			throws Exception {

		CLTmpActSiebelResponse response = new GetTmpactSiebelInfoAction(logger).execute(processName, maxRecord);
		context.getLogger().debug("getTmpActSiebelInfo->" + response.info().toString());

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
		private String processName;
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
			sql.append(" CL_TMP_ACT_SIEBEL").append(ConstantsDB.END_LINE);
			sql.append(" SET GEN_FLAG='Y'").append(ConstantsDB.END_LINE);			
			sql.append(" WHERE JOB_TYPE = ('").append(processName).append("') ").append(ConstantsDB.END_LINE);
			sql.append(" and GEN_FLAG = 'N'").append(ConstantsDB.END_LINE);
			return sql;
		}

		protected ExecuteResponse execute(BigDecimal maxRecord, String processName) {
			this.processName = processName;
			this.maxRecord = maxRecord;
			return executeUpdate(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public ExecuteResponse updateGenFileResultComplete(BigDecimal maxRecord, String processName,  Context context)
			throws Exception {

		ExecuteResponse response = new UpdateGenFileResultCompleteAction(logger).execute(maxRecord, processName);
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
	protected class InsertLetterOutBound extends DBTemplatesInsert<ExecuteResponse, UtilityLogger, DBConnectionPools> {

		public InsertLetterOutBound(UtilityLogger logger) {
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
			sql.append("INSERT INTO dbo.CL_TMP_ACT_SIEBEL ");
			sql.append("SELECT B.CA_NO, T.BA_NO, dbo.CL_F_GET_MOBILE_REF_BY_BA (B.BA_NO) AS REF_MOBILE_NO, B.CATEGORY, B.SUBCATEGORY, T.ACTION_STATUS_DTM, T.TREATMENT_ID, ");
			sql.append("'Letter – Outbound','DEBT',null, 'N',null");
			sql.append("FROM CL_TREATMENT T  ");
			sql.append("JOIN CL_ACTION A on T.ACTION_ID=A.ACTION_ID and ACTION_MODE =5 ");
			sql.append("JOIN CL_BA_INFO B on T.BA_NO=B.BA_NO ");
			sql.append("WHERE T.ACTION_STATUS=4 ");
			sql.append("AND T.ACTIVITY_LOG_BOO='N' ");
			sql.append("AND CONVERT(varchar(8),T.ACTION_STATUS_DTM,112)=CONVERT(varchar(8),DATEADD(day,-1,getdate()),112) ");
			return sql;
		}

		protected ExecuteResponse execute() {
			return executeUpdate(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public ExecuteResponse insertLetterOutBound(Context context) throws Exception {
		ExecuteResponse response = new InsertLetterOutBound(logger).execute();
		context.getLogger().debug("insertLetterOutBound->" + response.info().toString());

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
	protected class InsertDebtOutBound extends DBTemplatesInsert<ExecuteResponse, UtilityLogger, DBConnectionPools> {

		public InsertDebtOutBound(UtilityLogger logger) {
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
			sql.append("INSERT INTO dbo.CL_TMP_ACT_SIEBEL ");
			sql.append("SELECT B.CA_NO, T.BA_NO, dbo.CL_F_GET_MOBILE_REF_BY_BA (B.BA_NO) AS REF_MOBILE_NO, B.CATEGORY, B.SUBCATEGORY, T.ACTION_STATUS_DTM, T.TREATMENT_ID, ");
			sql.append("'Debt - Outbound','DEBT',null, 'N',null");
			sql.append("FROM CL_TREATMENT T  ");
			sql.append("JOIN CL_ACTION A on T.ACTION_ID=A.ACTION_ID and ACTION_MODE =2 ");
			sql.append("JOIN CL_BA_INFO B on T.BA_NO=B.BA_NO ");
			sql.append("WHERE T.ACTION_STATUS=3 ");
			sql.append("AND T.ACTIVITY_LOG_BOO='N' ");
			sql.append("AND CONVERT(varchar(8),T.ACTION_STATUS_DTM,112)=CONVERT(varchar(8),DATEADD(day,-1,getdate()),112) ");
			return sql;
		}

		protected ExecuteResponse execute() {
			return executeUpdate(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public ExecuteResponse insertDebtOutBound(Context context) throws Exception {
		ExecuteResponse response = new InsertDebtOutBound(logger).execute();
		context.getLogger().debug("insertDebtOutBound->" + response.info().toString());

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
}
