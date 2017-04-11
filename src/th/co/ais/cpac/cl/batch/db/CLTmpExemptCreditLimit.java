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

public class CLTmpExemptCreditLimit {
	protected final UtilityLogger logger;

	public CLTmpExemptCreditLimit(UtilityLogger logger) {
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

	public class CLTmpExemptCreditLimitInfo {
		protected CLTmpExemptCreditLimitInfo() {
		}
		private BigDecimal tmpId;
		private BigDecimal exemptCustomerId;
		private String caNo;
		private String baNo;
		private String mobileNo;
		private String exemptMode;
		private String exemptLevel;
		private String channel;
		private String effectiveDate;
		private String endDate;
		private String expireDate;
		private int duration;
		private String locationCode;
		private String reason;
		public BigDecimal getTmpId() {
			return tmpId;
		}
		public void setTmpId(BigDecimal tmpId) {
			this.tmpId = tmpId;
		}
		public BigDecimal getExemptCustomerId() {
			return exemptCustomerId;
		}
		public void setExemptCustomerId(BigDecimal exemptCustomerId) {
			this.exemptCustomerId = exemptCustomerId;
		}
		public String getCaNo() {
			return caNo;
		}
		public void setCaNo(String caNo) {
			this.caNo = caNo;
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
		public String getExemptMode() {
			return exemptMode;
		}
		public void setExemptMode(String exemptMode) {
			this.exemptMode = exemptMode;
		}
		public String getExemptLevel() {
			return exemptLevel;
		}
		public void setExemptLevel(String exemptLevel) {
			this.exemptLevel = exemptLevel;
		}
		public String getChannel() {
			return channel;
		}
		public void setChannel(String channel) {
			this.channel = channel;
		}
		public String getEffectiveDate() {
			return effectiveDate;
		}
		public void setEffectiveDate(String effectiveDate) {
			this.effectiveDate = effectiveDate;
		}
		public String getEndDate() {
			return endDate;
		}
		public void setEndDate(String endDate) {
			this.endDate = endDate;
		}
		public String getExpireDate() {
			return expireDate;
		}
		public void setExpireDate(String expireDate) {
			this.expireDate = expireDate;
		}
		public int getDuration() {
			return duration;
		}
		public void setDuration(int duration) {
			this.duration = duration;
		}
		public String getLocationCode() {
			return locationCode;
		}
		public void setLocationCode(String locationCode) {
			this.locationCode = locationCode;
		}
		public String getReason() {
			return reason;
		}
		public void setReason(String reason) {
			this.reason = reason;
		}


	}

	protected class TruncateExempBlDl extends DBTemplatesInsert<ExecuteResponse, UtilityLogger, DBConnectionPools> {

		public TruncateExempBlDl(UtilityLogger logger) {
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
			sql.append("Truncate Table CL_TMP_EXEMPT_CREDIT_LIMIT ");
			return sql;
		}

		protected ExecuteResponse execute() {
			return executeUpdate(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public ExecuteResponse truncateExempBlDl(Context context) throws Exception {
		ExecuteResponse response = new TruncateExempBlDl(logger).execute();
		context.getLogger().debug("truncateExempBlDl->" + response.info().toString());

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

	protected class InsertExempCreditLimit extends DBTemplatesInsert<ExecuteResponse, UtilityLogger, DBConnectionPools> {
		BigDecimal batchTypeId;
		public InsertExempCreditLimit(UtilityLogger logger) {
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
					"INSERT INTO CL_TMP_EXEMPT_CREDIT_LIMIT(EXEMPT_CUSTOMER_ID, CA_NO, BA_NO, MOBILE_NO, EXEMPT_MODE, EXEMPT_LEVEL, CHANNEL, EFFECTIVE_DATE, END_DATE, EXPIRED_DATE, DURATION, LOCATION_CODE, REASON, WORK_ORDER_ID, GEN_FLAG, GEN_DATETIME) ");
			sql.append("SELECT E.EXEMPT_CUSTOMER_ID, E.CA_NO,ISNULL(E.BA_NO,'') AS BA_NO,ISNULL(E.MOBILE_NO,'') AS MOBILE_NO, ");
			sql.append("CASE WHEN E.ACTION_ID = 0 ");
			sql.append("THEN (SELECT L.CONDITION_4 FROM CL_CFG_LOV L WHERE L.LOV_KEYWORD = 'ACTION_MODE' AND L.LOV_KEYVALUE = E.ACTION_MODE) ");			
			sql.append("ELSE (SELECT A.ACTION_ABRV FROM CL_ACTION A WHERE A.ACTION_ID = E.ACTION_ID) END AS MODE, ");			
			sql.append("CASE E.EXEMPT_LEVEL WHEN 4 THEN 'Mobile' WHEN 3 THEN 'BA' WHEN 2 THEN 'SA' WHEN 1 THEN 'CA' END AS EXEMP_LEVEL, ");			
			sql.append("'Collection' AS CHANNEL, ");	
			sql.append("ISNULL(E.EXEMPT_APPRV_DTM, E.EXEMPT_START_DTM) AS EFFECTIVE_DATE, ");				
			sql.append("ISNULL(E.EXEMPT_EXPIRE_DTM, E.EXEMPT_END_DTM) AS END_DATE, ");	
			sql.append("ISNULL(E.EXEMPT_EXPIRE_DTM, E.EXEMPT_END_DTM) AS EXPIRED_DATE, ");	
			sql.append("DATEDIFF(dd,ISNULL(E.EXEMPT_APPRV_DTM, E.EXEMPT_START_DTM), ISNULL(E.EXEMPT_EXPIRE_DTM, E.EXEMPT_END_DTM)) as DURATION, ");				
			sql.append("ISNULL(ISNULL(T.APPROVED_LOCATION,T.CREATED_LOCATION),T.LAST_UPD_LOCATION) as LOCATION, ");				
			sql.append("(SELECT SUBSTRING(REASON_NAME,1,30) FROM CL_REASON R WHERE T.EXEMPT_REASON_ID=R.REASON_ID) as REASON, ");	
			sql.append("null,'N' ,null ");
			sql.append("FROM CL_EXEMPT T, CL_EXEMPT_CUSTOMER E ");	
			sql.append("WHERE T.EXEMPT_ID = E.EXEMPT_ID ");	
			sql.append("AND T.EXEMPT_STATUS = 1 ");				
			sql.append("AND E.EXEMPT_STATUS = 1 ");	
			sql.append("AND CONVERT(varchar(8),ISNULL(E.EXEMPT_APPRV_DTM,E.CREATED),112)=CONVERT(varchar(8),DATEADD(day,-1,getdate()),112)  ");				
			sql.append("AND (E.ACTION_MODE = 13)  ");				
			sql.append("AND E.EXEMPT_LEVEL !=2 ");				
			sql.append("AND NOT EXISTS (SELECT * FROM CL_BATCH_EXEMPT BE, CL_BATCH B WHERE BE.BATCH_ID = B.BATCH_ID AND BE.EXEMPT_CUSTOMER_ID = E.EXEMPT_CUSTOMER_ID AND B.BATCH_TYPE_ID =").append(batchTypeId).append(")");;
			return sql;
		}

		protected ExecuteResponse execute(BigDecimal batchTypeId) {
			this.batchTypeId = batchTypeId;
			return executeUpdate(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public ExecuteResponse insertExempCreditLimit(Context context,BigDecimal batchTypeId) throws Exception {
		ExecuteResponse response = new InsertExempCreditLimit(logger).execute(batchTypeId);
		context.getLogger().debug("insertExempBlDl->" + response.info().toString());

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

	public class CLTmpExemptCreditLimitResponse extends DBTemplatesResponse<ArrayList<CLTmpExemptCreditLimitInfo>> {

		@Override
		protected ArrayList<CLTmpExemptCreditLimitInfo> createResponse() {
			return new ArrayList<>();
		}

	}

	protected class GetTmpExemptCreditLimitInfoAction
			extends DBTemplatesExecuteQuery<CLTmpExemptCreditLimitResponse, UtilityLogger, DBConnectionPools> {
		private BigDecimal maxRecord;

		public GetTmpExemptCreditLimitInfoAction(UtilityLogger logger) {
			super(logger);
		}

		@Override
		protected CLTmpExemptCreditLimitResponse createResponse() {
			return new CLTmpExemptCreditLimitResponse();
		}

		@Override
		protected StringBuilder createSqlProcess() {
			StringBuilder sql = new StringBuilder();
			sql.append(" SELECT TOP ").append(maxRecord).append(ConstantsDB.END_LINE);
			sql.append(" TMP_ID,EXEMPT_CUSTOMER_ID, CA_NO, BA_NO, MOBILE_NO, EXEMPT_MODE, EXEMPT_LEVEL, CHANNEL, EFFECTIVE_DATE, END_DATE, EXPIRED_DATE, DURATION, LOCATION_CODE, REASON ")
					.append(ConstantsDB.END_LINE);
			sql.append(" FROM CL_TMP_EXEMPT_CREDIT_LIMIT ").append(ConstantsDB.END_LINE);
			sql.append(" WHERE GEN_FLAG = 'N'").append(ConstantsDB.END_LINE);
			return sql;
		}

		@Override
		protected void setReturnValue(ResultSet resultSet) throws SQLException {
			CLTmpExemptCreditLimitInfo temp = new CLTmpExemptCreditLimitInfo();
			temp.setTmpId(resultSet.getBigDecimal("TMP_ID"));
			temp.setExemptCustomerId(resultSet.getBigDecimal("EXEMPT_CUSTOMER_ID"));
			temp.setCaNo(resultSet.getString("CA_NO"));
			temp.setBaNo(resultSet.getString("BA_NO"));
			temp.setMobileNo(resultSet.getString("MOBILE_NO"));
			temp.setExemptMode(resultSet.getString("EXEMPT_MODE"));
			temp.setExemptLevel(resultSet.getString("EXEMPT_LEVEL"));
			temp.setChannel(resultSet.getString("CHANNEL"));
			temp.setEffectiveDate(Utility.convertDateToString(resultSet.getDate("EFFECTIVE_DATE"), "ddMMyyyy"));
			temp.setEndDate(Utility.convertDateToString(resultSet.getDate("END_DATE"), "ddMMyyyy"));
			temp.setExpireDate(Utility.convertDateToString(resultSet.getDate("EXPIRED_DATE"), "ddMMyyyy"));
			temp.setDuration(resultSet.getInt("DURATION"));
			temp.setLocationCode(resultSet.getString("LOCATION_CODE"));
			temp.setReason(resultSet.getString("REASON"));
			response.getResponse().add(temp);
		}

		protected CLTmpExemptCreditLimitResponse execute(BigDecimal maxRecord) {
			this.maxRecord = maxRecord;
			return executeQuery(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public CLTmpExemptCreditLimitResponse getTmpExemptCreditLimitInfo(BigDecimal maxRecord, Context context) throws Exception {

		CLTmpExemptCreditLimitResponse response = new GetTmpExemptCreditLimitInfoAction(logger).execute(maxRecord);
		context.getLogger().debug("getTmpExemptCreditLimitInfo->" + response.info().toString());

		switch (response.getStatusCode()) {
		case CLTmpExemptCreditLimitCountResponse.STATUS_COMPLETE: {
			break;
		}
		case CLTmpExemptCreditLimitCountResponse.STATUS_DATA_NOT_FOUND: {
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
			sql.append(" CL_TMP_EXEMPT_CREDIT_LIMIT").append(ConstantsDB.END_LINE);
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
	
	public class CLTmpExemptCreditLimitCount {
		protected CLTmpExemptCreditLimitCount() {
		}
		private int totalRecord;
		public int getTotalRecord() {
			return totalRecord;
		}
		public void setTotalRecord(int totalRecord) {
			this.totalRecord = totalRecord;
		}
	}
	public class CLTmpExemptCreditLimitCountResponse extends DBTemplatesResponse<ArrayList<CLTmpExemptCreditLimitCount>> {

		@Override
		protected ArrayList<CLTmpExemptCreditLimitCount> createResponse() {
			return new ArrayList<>();
		}

	}

	protected class GetTmpExemptCreditLimitCountAction
			extends DBTemplatesExecuteQuery<CLTmpExemptCreditLimitCountResponse, UtilityLogger, DBConnectionPools> {

		public GetTmpExemptCreditLimitCountAction(UtilityLogger logger) {
			super(logger);
		}

		@Override
		protected CLTmpExemptCreditLimitCountResponse createResponse() {
			return new CLTmpExemptCreditLimitCountResponse();
		}

		@Override
		protected StringBuilder createSqlProcess() {
			StringBuilder sql = new StringBuilder();
			sql.append(" SELECT COUNT(*) AS CNT ").append(ConstantsDB.END_LINE);
			sql.append(" FROM CL_TMP_EXEMPT_CREDIT_LIMIT ").append(ConstantsDB.END_LINE);
			sql.append(" WHERE GEN_FLAG = 'N'").append(ConstantsDB.END_LINE);
			return sql;
		}

		@Override
		protected void setReturnValue(ResultSet resultSet) throws SQLException {
			CLTmpExemptCreditLimitCount temp = new CLTmpExemptCreditLimitCount();
			temp.setTotalRecord(resultSet.getInt("CNT"));
			response.getResponse().add(temp);
		}

		protected CLTmpExemptCreditLimitCountResponse execute() {
			return executeQuery(ConstantsDB.getDBConnectionPools(logger), true);
		}
	}

	public int getTmpExemptCreditLimitCount(Context context) throws Exception {

		CLTmpExemptCreditLimitCountResponse response = new GetTmpExemptCreditLimitCountAction(logger).execute();
		context.getLogger().debug("getTmpExemptCreditLimitCount->" + response.info().toString());

		switch (response.getStatusCode()) {
		case CLTmpExemptCreditLimitCountResponse.STATUS_COMPLETE: {
			break;
		}
		case CLTmpExemptCreditLimitCountResponse.STATUS_DATA_NOT_FOUND: {
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
