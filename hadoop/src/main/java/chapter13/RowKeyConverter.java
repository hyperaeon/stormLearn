package chapter13;

import org.apache.hadoop.hbase.util.Bytes;

public class RowKeyConverter {

	private static final int STATION_ID_LENGTH = 12;
	
	public static byte[] makeObservationRowkey(String stationId, long observationTime) {
		byte[] row = new byte[STATION_ID_LENGTH + Bytes.SIZEOF_LONG];
		Bytes.putBytes(row, 0, Bytes.toBytes(stationId), 0, STATION_ID_LENGTH);
		long reverseOrderEpoch = Long.MAX_VALUE - observationTime;
		Bytes.putLong(row, STATION_ID_LENGTH, reverseOrderEpoch);
		return row;
	}
}
