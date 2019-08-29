package io.mycat.route.function;

import io.mycat.config.model.rule.RuleAlgorithm;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

/**
 * 例子 按半个月份分区 ，每半个月一个分片，即每个月的1到15号为一个分片，其余的为另一个分片
 *
 * 如果第一个分片是1月1日，那么每日对应的分片索引如下：
 *  0 - 1月 1-15, 7月 1-15
 *  1 - 1月 16-31, 7月 16-31
 *  2 - 2月 1-15, 8月 1-15
 *  3 - 2月 16-28/29, 8月 16-31
 *  4 - 3月 1-15, 9月 1-15
 *  5 - 3月 16-31, 9月 16-30
 *  6 - 4月 1-15, 10月 1-15
 *  7 - 4月 16-30, 10月 16-31
 *  8 - 5月 1-15, 11月 1-15
 *  9 - 5月 16-31, 11月 16-30
 * 10 - 6月 1-15, 12月 1-15
 * 11 - 6月 16-30, 12月 16-31
 *
 * @author wzh
 *
 */
public class PartitionByHalfMonth extends AbstractPartitionAlgorithm implements
		RuleAlgorithm {
	private static final Logger LOGGER = LoggerFactory.getLogger(PartitionByDate.class);
	private String sBeginDate;
	/** 默认格式 */
	private String dateFormat = "yyyy-MM-dd";
	/** 场景 */
	private int scene = -1;
	private String sEndDate;
	private Calendar beginDate;
	private Calendar endDate;
	/** 分片数 */
	private int nPartition = 12;

	private ThreadLocal<SimpleDateFormat> formatter;

	@Override
	public void init() {
		try {
			if (StringUtil.isEmpty(sBeginDate) && StringUtil.isEmpty(sEndDate)) {
				// 如果没有配置开始时间和结束时间，则将1月份设为开始时间，将12月份设为结束时间
				nPartition = 12;
				scene = 1;
				initFormatter();
				beginDate = Calendar.getInstance();
				beginDate.set(Calendar.MONTH, 0);
				endDate = Calendar.getInstance();
				endDate.set(Calendar.MONTH, 11);
				return;
			}
			beginDate = Calendar.getInstance();
			beginDate.setTime(new SimpleDateFormat(dateFormat)
									  .parse(sBeginDate));
			initFormatter();
			if(sEndDate!=null&&!sEndDate.equals("")) {
				endDate = Calendar.getInstance();
				endDate.setTime(new SimpleDateFormat(dateFormat).parse(sEndDate));

//				nPartition = ((endDate.get(Calendar.YEAR) - beginDate.get(Calendar.YEAR)) * 12
//						+ endDate.get(Calendar.MONTH) - beginDate.get(Calendar.MONTH)) + 1;
				nPartition = 12;
				if (nPartition <= 0) {
					throw new IllegalArgumentException("Incorrect time range for month partitioning!");
				}
			} else {
				nPartition = 12;
			}
		} catch (ParseException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private void initFormatter() {
		formatter = new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat(dateFormat);
            }
        };
	}

	/**
	 * 对于循环分区，需要旋转目标分区的计算值以适合分区范围
	 * @return 0-11
	 */
	private int reCalculatePartition(int targetPartition) {
		/**
		 * 如果目标日期是分区设置开始时间的前一个时间点，则将目标和开始日期之间的增量范围移到正值
		 */
		if (targetPartition < 0) {
			targetPartition = nPartition - (-targetPartition) % nPartition;
		}

		//if (targetPartition >= nPartition) {
		targetPartition =  targetPartition % nPartition;
		//}
		return targetPartition;
	}

	@Override
	public Integer calculate(String columnValue)  {
		try {
			if (scene == 1) {
				Calendar curTime = Calendar.getInstance();
				curTime.setTime(formatter.get().parse(columnValue));
				// 月份 从0开始，0表示1月
				int month = curTime.get(Calendar.MONTH);
				// 日期
				int day = curTime.get(Calendar.DAY_OF_MONTH);
				int targetPartition = month * 2 + (day>15 ? 1 : 0);
				targetPartition = reCalculatePartition(targetPartition);
				return targetPartition;
			}
			int targetPartition;
			Calendar curTime = Calendar.getInstance();
			curTime.setTime(formatter.get().parse(columnValue));
			// 计算当期时间离开始时间相隔多少个半月
			targetPartition = ( (curTime.get(Calendar.YEAR) - beginDate.get(Calendar.YEAR))* 24 + curTime.get(Calendar.MONTH) * 2 + (curTime.get(Calendar.DAY_OF_MONTH)>15 ? 1 : 0) ) - ( beginDate.get(Calendar.MONTH) * 2 + (beginDate.get(Calendar.DAY_OF_MONTH)>15 ? 1 : 0) );

			/**
			 * 对于循环分区，需要旋转目标分区的计算值以适合分区范围
			 */
			// 谢忠江修改了，按月分片的数据表，不在使用新的分片表，而是12个月循环，到第13个月就需要回到第1个月的数据表
			if (nPartition > 0) {
				targetPartition = reCalculatePartition(targetPartition);
			}
			return targetPartition;

		} catch (ParseException e) {
			throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue).append(" Please check if the format satisfied.").toString(),e);
		}
	}

	@Override
	public Integer[] calculateRange(String beginValue, String endValue) {
		try {
			return doCalculateRange(beginValue, endValue,beginDate);
		} catch (ParseException e) {
			LOGGER.error("error",e);
			return new Integer[0];
		}
	}

	private Integer[] doCalculateRange(String beginValue, String endValue,Calendar beginDate) throws ParseException {
		int startPartition, endPartition;
		Calendar partitionTime = Calendar.getInstance();
		SimpleDateFormat format = new SimpleDateFormat(dateFormat);

		partitionTime.setTime(format.parse(beginValue));
		if(partitionTime.before(beginDate)){
			return new Integer[0];
		}
		startPartition = ( (partitionTime.get(Calendar.YEAR) - beginDate.get(Calendar.YEAR))* 24 + partitionTime.get(Calendar.MONTH) * 2 + (partitionTime.get(Calendar.DAY_OF_MONTH)>15 ? 1 : 0) ) - ( beginDate.get(Calendar.MONTH) * 2 + (beginDate.get(Calendar.DAY_OF_MONTH)>15 ? 1 : 0) );

		partitionTime.setTime(format.parse(endValue));
		if(partitionTime.before(beginDate)){
			return new Integer[0];
		}
		endPartition = ( (partitionTime.get(Calendar.YEAR) - beginDate.get(Calendar.YEAR))* 24 + partitionTime.get(Calendar.MONTH) * 2 + (partitionTime.get(Calendar.DAY_OF_MONTH)>15 ? 1 : 0) ) - ( beginDate.get(Calendar.MONTH) * 2 + (beginDate.get(Calendar.DAY_OF_MONTH)>15 ? 1 : 0) );

		int[] partitionArr = new int[nPartition];
		List<Integer> list = new ArrayList<>();

		while (startPartition <= endPartition) {
			Integer nodeValue = reCalculatePartition(startPartition);
			if(partitionArr[nodeValue]!=1){
				partitionArr[nodeValue] = 1;
				list.add(nodeValue);
			}
			startPartition++;
		}
		int size = list.size();
		// 当在场景1： "2015-01-01", "2014-04-03" 范围出现的时候
		// 是应该返回null 还是返回 [] ?
		return (list.toArray(new Integer[size]));
	}

	@Override
	public int getPartitionNum() {
		return this.nPartition;
	}

	public void setsBeginDate(String sBeginDate) {
		this.sBeginDate = sBeginDate;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public void setsEndDate(String sEndDate) {
		this.sEndDate = sEndDate;
	}

}
