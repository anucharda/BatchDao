/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package th.co.ais.cpac.cl.batch;

import th.co.ais.cpac.cl.batch.cnf.CNFDatabase;
import th.co.ais.cpac.cl.common.UtilityLogger;
import th.co.ais.cpac.cl.template.database.DBConnectionPools;

/**
 *
 * @author Sirirat
 */
public class ConstantsDB {
	public static String END_LINE = "";
	public static int actInprogressStatus = 3;
	public static int batchCompleteStatus = 3;

	public static DBConnectionPools getDBConnectionPools(UtilityLogger logger) {
		CNFDatabase cnf = new CNFDatabase();
		return new DBConnectionPools<>(cnf, logger);
	}

	public enum Environment {
		PROD(1), DEV(2), SIT(3), UnKnow(-9999);
		private final int code;

		private Environment(int code) {
			this.code = code;
		}

		public int getCode() {
			return code;
		}
	}

	public static final Environment mapEnvironment(int code) {
		if (Environment.PROD.getCode() == code) {
			return Environment.PROD;
		} else if (Environment.DEV.getCode() == code) {
			return Environment.DEV;
		} else if (Environment.SIT.getCode() == code) {
			return Environment.SIT;
		}
		return Environment.UnKnow;
	}

	public enum OutboundStatus {
		NoOutboundResponse(0), Generating(1), Complete(2), Failed(3), Cancelled(4), UnKnow(-9999);
		private final int code;

		private OutboundStatus(int code) {
			this.code = code;
		}

		public int getCode() {
			return code;
		}
	}
	 public enum InboundStatus {
		    NoInboundResponse(0),
		    Pending(1),
		    Received(2),
		    Complete(3),
		    UnKnow(-9999);
		    private final int code;

		    private InboundStatus(int code) {
		      this.code = code;
		    }

		    public int getCode() {
		      return code;
		    }
		  }
}
