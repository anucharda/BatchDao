package th.co.ais.cpac.cl.batch.db;

import java.math.BigDecimal;
import java.util.Date;

import th.co.ais.cpac.cl.batch.ConstantsDB;
import th.co.ais.cpac.cl.batch.db.CLBatch.CLBatchInfo;
import th.co.ais.cpac.cl.common.UtilityLogger;
import th.co.ais.cpac.cl.template.database.DBConnectionPools;
import th.co.ais.cpac.cl.template.database.DBTemplatesInsert;
import th.co.ais.cpac.cl.template.database.DBTemplatesResponse;

public class CLBatchExempt {
	protected final UtilityLogger logger;

	public CLBatchExempt(UtilityLogger logger) {
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

	public class CLBatchExemptInfo {

		protected CLBatchExemptInfo() {
		}

		private BigDecimal batchId;
		private BigDecimal exemptCustomerId;
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

		public BigDecimal getExemptCustomerId() {
			return exemptCustomerId;
		}

		public void setExemptCustomerId(BigDecimal exemptCustomerId) {
			this.exemptCustomerId = exemptCustomerId;
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
	}

	protected class InsertCLBatchExemptProcess
			extends DBTemplatesInsert<ExecuteResponse, UtilityLogger, DBConnectionPools> {

		private CLBatchExemptInfo batchExemptInfo;

		public InsertCLBatchExemptProcess(UtilityLogger logger) {
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

			genNumber("BATCH_ID", batchExemptInfo.getBatchId(), null, column, value, false);
			genNumber("EXEMPT_CUSTOMER_ID", batchExemptInfo.getExemptCustomerId(), null, column, value, false);
			genMethod("CREATED", "getdate()", column, value, false);
			genString("CREATED_BY", batchExemptInfo.getCreatedBy(), column, value, false);
			genMethod("LAST_UPD", "getdate()", column, value, false);
			genString("LAST_UPD_BY", batchExemptInfo.getLastUpdBy(), column, value, false);

			StringBuilder sql = new StringBuilder();
			sql.append("INSERT INTO CL_BATCH_EXEMPT(").append(column).append(")");
			sql.append("VALUES(").append(value).append(")");

			return sql;
		}

		protected ExecuteResponse execute(CLBatchExemptInfo batchExemptInfo) {
			this.batchExemptInfo = batchExemptInfo;
			return super.executeUpdateGetIdentity(ConstantsDB.getDBConnectionPools(logger), true);
		}

	}

	public CLBatchExemptInfo buildCLBatchExemptInfo() {
		return new CLBatchExemptInfo();
	}

	public ExecuteResponse insertCLBatchExempt(CLBatchExemptInfo batchExmptInfo) {
		return new InsertCLBatchExemptProcess(logger).execute(batchExmptInfo);
	}
}
