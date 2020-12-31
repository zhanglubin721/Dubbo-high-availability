package com.yicaida.mfbi.controller;

import cn.afterturn.easypoi.excel.ExcelImportUtil;
import cn.afterturn.easypoi.excel.entity.ImportParams;
import com.yicaida.constant.DBTypeConstant;
import com.yicaida.dal.entity.DBInfoBean;
import com.yicaida.dal.entity.nodb.ResultBody;
import com.yicaida.dal.service.DynamicConnServiceV2;
import com.yicaida.dal.service.SQLMapperService;
import com.yicaida.etl.constant.EtlConstant;
import com.yicaida.etl.entity.DirectoryBean;
import com.yicaida.etl.entityself.InsertUpdateBean;
import com.yicaida.etl.entityself.TableInputOutField;
import com.yicaida.etl.packageservice.trans.steps.*;
import com.yicaida.etl.service.JobService;
import com.yicaida.etl.service.PackageService;
import com.yicaida.etl.service.TranService;
import com.yicaida.mdm.service.MdmService;
import com.yicaida.mfbi.common.Common;
import com.yicaida.mfbi.common.MfbiCommon;
import com.yicaida.mfbi.entity.PageBean;
import com.yicaida.mfbi.entity.SubjectBean;
import com.yicaida.mfbi.service.*;
import com.yicaida.mfbi.service.dwservice.DwOracle;
import com.yicaida.sso.entity.UserBean;
import com.yicaida.utils.AesUtil;
import com.yicaida.utils.DataUtil;
import net.sf.json.JSONArray;
import net.sf.json.JSONNull;
import net.sf.json.JSONObject;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Description;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.util.*;
/*import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;*/

/**
 * 贴源库处理
 * 
 * @author ssqd
 * 
 */
@Controller
@RequestMapping("/cdw")
public class CDWController {

	/*
	 * @Autowired private CDWService cdwService;
	 */

	@Autowired
	private SQLMapperService sqlMapperService;
	@Autowired
	private DynamicConnServiceV2 dynamicConnServiceV2;
	@Autowired
	private DwService dwService;
	@Autowired
	private MdmService mdmService;
	@Autowired
	private DLSService dlsService;
	@Autowired
	private PackageService packageService;
	@Autowired
	private DwOracle dwOracle;
	@Autowired
	private CommonService commonService;
	@Autowired
	private RawService rawService;

	@Autowired
	private DimService dimService;

	@Autowired
	private MfbiCommonService mfbicommonService;

	@Autowired
	private JobService jobService;

	@Autowired
	private TranService tranService;


	/***************************** 数据分类 **************************************/

	/**
	 * 数据分类list
	 * 
	 * @param request
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/dataCategoryList")
	public List<Map<String, Object>> dataCategoryList(HttpServletRequest request,String rawType) {
		try {
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			String rawType_ = " ";
			if(rawType != null && !"".equals(rawType)){
				rawType_ = " and RAWTYPE='"+rawType+"'";
			}else{
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("ID", "#");
				map.put("CNAME", "全部分类");
				map.put("PID", null);
				list.add(map);
			}
			String sql = "select * from raw_datacategory where PROVINCE='" + user.optAdmdivCode + "' "+ rawType_ +"  order by cname";
			List<Map<String, Object>> listData = sqlMapperService.selectList(sql);
			if (listData != null) {
				list.addAll(listData);
			}
			return list;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 单个数据分类
	 * 
	 * @param request
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/dataCategory")
	public Map<String, Object> dataCategory(HttpServletRequest request, String ztreeId) {
		try {
			Map<String, Object> listData = sqlMapperService.selectOne("select * from RAW_DATACATEGORY where ID = '" + ztreeId + "' order by ccode");

			return listData;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 分类描述
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getDataCategory")
	public Map<String, Object> getDataCategory(HttpServletRequest request, String id) {
		try {
			return sqlMapperService.selectOne("select * from RAW_DATACATEGORY where id='" + id + "'");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/***
	 * 增加或修改数据分类
	 * 
	 * @param request
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/addOrUpdateDataCategory")
	public ResultBody addOrUpdateDataPipeline(HttpServletRequest request) {
		try {
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
			String id = request.getParameter("id");
			String optAdmdiv = user.optAdmdiv;
			String optAdmdivCode = user.optAdmdivCode;
			String cCode = request.getParameter("cCode"); // 分类编码
			String cName = request.getParameter("cName"); // 分类名称
			String pId = request.getParameter("pId");
			String rawType = request.getParameter("rawType");
			String sql = "";
			if (id != null) {
				sql = " UPDATE RAW_datacategory SET ccode = '" + cCode + "', cname = '" + cName + "',PROVINCE='" + optAdmdivCode + "',ADMDIV='" + optAdmdiv + "' WHERE ID = '" + id + "'";
			} else {
				sql = " INSERT INTO RAW_datacategory ( ccode, cname, pid,ADMDIV,province,RAWTYPE) VALUES('" + cCode + "', '" + cName + "', '" + pId + "', '" + optAdmdiv + "','" + optAdmdivCode + "','"+rawType+"')";
			}

			if (id != null) { // 修改
				String rawDirectories = this.getRawDirectories(pId);
				String oldcName = sqlMapperService.selectOne("select cname from RAW_datacategory where id='" + id + "'", String.class);
				DirectoryBean dirBean = new DirectoryBean(rawDirectories + "/" + cName, DirectoryBean.TYPE_EDIT, rawDirectories + "/" + oldcName);
				packageService.saveDirectory(optAdmdivCode, dirBean);
			} else {
				String rawDirectories = this.getRawDirectories(pId);
				DirectoryBean dirBean = new DirectoryBean(rawDirectories + "/" + cName);
				packageService.saveDirectory(optAdmdivCode, dirBean);
			}

			sqlMapperService.execSql(sql);
			return ResultBody.createSuccessResult(id != null ? "数据分类信息更新成功！" : ("成功增加数据分类：</br>" + cName));
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("操作失败！原因：" + e.getMessage());
		}
	}

	/**
	 * 获取中心仓库目录，从最上层向下排列
	 * 
	 * @param categoryId
	 *            当前所选分类（目录）id
	 * @return /xx分类/yy分类..../当前所选目录
	 */
	private String getRawDirectories(String categoryId) {
		String folderName = MfbiCommon.SYSDIRECTORY + "/" + MfbiCommon.SOURCE_TYK;
		String sql = "select t3.cname" + " from raw_datacategory t3 start with t3.id in" + " (select ID from raw_datacategory where ID = '" + categoryId + "')" + " connect by  t3.id =  PRIOR t3.pid";
		List<Map<String, Object>> selectList = sqlMapperService.selectList(sql);
		for (int x = selectList.size() - 1; x >= 0; x--) {
			folderName += "/" + selectList.get(x).get("CNAME");
		}
		return folderName;
	}

	@ResponseBody
	@RequestMapping("/removeDataCategory")
	public ResultBody removeDataPipeline(HttpServletRequest request) {
		try {
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
			String id = request.getParameter("id");
			List<Map<String, Object>> dataList = sqlMapperService.selectList(" select * from RAW_centrewarehouse where DATACATEGORYID='" + id + "' ORDER BY CREATEDATE DESC");
			if (dataList.size() > 0) {
				return ResultBody.createErrorResult("该分类下存在贴源库，不支持此操作！");
			}

			String rawDirectories = this.getRawDirectories(id);
			DirectoryBean dirBean = new DirectoryBean(null, DirectoryBean.TYPE_REMOVE, rawDirectories);
			packageService.saveDirectory(user.optAdmdivCode, dirBean);

			sqlMapperService.execSql(" DELETE RAW_datacategory WHERE id='" + id + "'");
			return ResultBody.createSuccessResult("操作成功！");
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("操作失败！");
		}
	}

	/***************************** 数据分类end **************************************/

	/***************************** 库 *****************************************/

	/**
	 * 中心数据仓库list
	 *
	 * @param request
	 * @param dataCategoryId
	 *            数据分类id
	 * @param cname
	 *            搜索的仓库中文名称id
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/cdwList")
	public List<Map<String, Object>> cdwList(HttpServletRequest request, String dataCategoryId, String cname) {
		try {
			if(cname == null) {
				cname = "";
			}
			return sqlMapperService.selectList(
					"select GUID,REMARK,CNAME,DATACATEGORYID,CREATER,SOURCETYPE, to_char(createdate,'yyyy-mm-dd  hh24:mm:ss' ) as CREATEDATE from RAW_centrewarehouse "
							+ "where DATACATEGORYID='" + dataCategoryId + "' and CNAME like '%" + cname + "%' ORDER BY CREATEDATE asc, CNAME");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 中心数据仓库list
	 *
	 * @param request
	 * @param centreWarehouseId
	 *            仓库id
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getTableAndViewCount")
	public Map<String, Object> getTableAndViewCount(HttpServletRequest request, String centreWarehouseId) {
		try {
			if(centreWarehouseId == null || "".equals(centreWarehouseId)) {
				return null;
			}
			Map<String, Object> tableMap = sqlMapperService.selectOne("SELECT count(1) as tableNum from RAW_CWTABLE T where T.centreWarehouseId = '" + centreWarehouseId + "' and IS_VIEW = 0");
			Map<String, Object> viewMap = sqlMapperService.selectOne("SELECT count(1) as viewNum from RAW_CWTABLE T where T.centreWarehouseId = '" + centreWarehouseId + "' and IS_VIEW = 1");
			HashMap<String, Object> resultMap = new HashMap<>();
			resultMap.put("TABLENUM", tableMap.get("TABLENUM"));
			resultMap.put("VIEWNUM", viewMap.get("VIEWNUM"));
			return resultMap;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 查询贴源库,中心仓库,数据集市数量
	 * 
	 * @param request
	 * @param dataCategoryId
	 *            数据分类id
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/indexInfoCount")
	public Map<String, Object> tykCount(HttpServletRequest request) {
		try {
			return sqlMapperService
					.selectOne("SELECT (SELECT COUNT(1) from bi_subject t WHERE t.basetype = '1') AS type1, (SELECT COUNT(1) from bi_subject t WHERE t.basetype = '2') AS type2, (SELECT COUNT(1) from Raw_Centrewarehouse) AS type3 FROM dual");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 仓库描述
	 * 
	 * @param request
	 * @param guid
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getCDW")
	public Map<String, Object> getCDW(HttpServletRequest request, String guid) {
		try {
			return sqlMapperService
					.selectOne("select GUID, REMARK, CNAME, DATACATEGORYID, CREATER, to_char(CREATEDATE,'yyyy-MM-dd HH:mi:ss') as CREATEDATE, SOURCETYPE,RESTOREDWGUID from RAW_centrewarehouse where guid='" + guid
							+ "'");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 注冊的贴源库，设置了还原库后对已注册的表增加转换
	 * @param request
	 */
	@ResponseBody
	@RequestMapping("/restoredwPipe")
	public void restoredwPipe(HttpServletRequest request) {
		try {
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
			String guid = request.getParameter("guid");
			String cName = request.getParameter("cName"); // 仓库中文名称
			String restoredwPipe = request.getParameter("restoredwPipe"); // 还原库管道id
			String dataCategoryId = request.getParameter("dataCategoryId"); // 分类Id
			mfbicommonService.restoredwPipe(guid,cName,restoredwPipe,user.optAdmdivCode,dataCategoryId);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/***
	 * 增加或修改仓库
	 * 
	 * @param request
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/addOrUpdateCDW")
	public ResultBody addOrUpdateCDW(HttpServletRequest request) {
		try {
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
			String dataCategoryId = request.getParameter("dataCategoryId"); // 分类Id
			String guid = request.getParameter("guid");
			String remark = request.getParameter("remark"); // 备注
			String cName = request.getParameter("cName"); // 仓库中文名称
			String pipelineName = request.getParameter("PIPELINENAME"); // 仓库中文名称
			String pipelineid = request.getParameter("pipelineid");
			String dbtype = request.getParameter("dbtype");
			String dbinfo = request.getParameter("dbinfo");
			String sourceType = request.getParameter("sourceType"); // 02:注册库
			String province = user.optAdmdivCode;
			String guidNew = null;
			if (guid != null) { // 修改
				String rawDirectories = this.getRawDirectories(dataCategoryId);
				String oldcName = sqlMapperService.selectOne("select cname from RAW_centrewarehouse where guid='" + guid + "'", String.class);
				DirectoryBean dirBean = new DirectoryBean(rawDirectories + "/" + cName, DirectoryBean.TYPE_EDIT, rawDirectories + "/" + oldcName);
				packageService.saveDirectory(province, dirBean); // 维护转换大目录
				mfbicommonService.updateDLSInfo_database(MfbiCommon.APPID_RAW, guid, cName);
			} else {
				guidNew = sqlMapperService.selectOne("select sys_guid() from dual", String.class);
				String rawDirectories = this.getRawDirectories(dataCategoryId);
				DirectoryBean dirBean = new DirectoryBean(rawDirectories + "/" + cName, 1);
				ResultBody res = packageService.saveDirectory(province, dirBean); // 维护转换大目录
				if(res.isError)
					return res;
				// 新增仓库级公共作业
				res = jobService.createEmptyJob(user.optAdmdivCode, cName, rawDirectories + "/" + cName);
				if(res.isError)
					return res;
				String jobId = ((Map<String,Object>)res.result).get("jobId").toString();
				// 记录作业id到mfbi转换信息中间表，贴源库增加表后转换加入到该作业中
				mfbicommonService.create_eltransinfo(user.optAdmdivCode,Integer.valueOf(jobId), guidNew, guidNew, "00", "2", user.UserName, "0");
			}

			String sql = "";
			List<String> sqls = new ArrayList<String>();
			if (guid != null) {// 修改
				sql = " UPDATE RAW_centrewarehouse SET REMARK = '" + remark + "',cname = '" + cName + "',datacategoryid = '" + dataCategoryId + "',PROVINCE='" + province + "',ALTERER='"
						+ user.UserName + "',ALTERERID='" + user.MID + "',UPDATEDATE=sysdate,RESTOREDWGUID='' WHERE guid='" + guid + "'";
				sqls.add(sql);
			} else {// 新增
				sql = " INSERT INTO RAW_centrewarehouse (guid, cname, REMARK, datacategoryid, CREATER,sourceType,PROVINCE,CREATERID) VALUES('" + guidNew + "','" + cName + "', '" + remark + "', '"
						+ dataCategoryId + "', '" + user.UserName + "','" + sourceType + "','" + province + "','" + user.MID + "')";
				String sql_pipel1 = "insert into BI_T_DBINFO (CENTREWAREHOUSEID, PIPELINEID, PIPELINENAME, DBTYPE, DBINFO,CREATER,CREATERID) " + " values('" + guidNew + "', '" + pipelineid + "', '"
						+ pipelineName + "','" + dbtype + "','" + dbinfo + "','" + user.UserName + "','" + user.MID + "')";
				sqls.add(sql);
				// 贴源库表分类 -默认分类
				sqls.add("insert into raw_cwtablecat(GUID,NAME,CENTREWAREHOUSEID,ORDERNUM) values('default','0-默认分类','" + guidNew + "',0)");
				sqls.add(sql_pipel1);
			}
			sqlMapperService.execSqls2(sqls);
			return ResultBody.createSuccessResult(guid == null ? guidNew : guid);
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("操作失败！原因：" + e.getMessage());
		}
	}

	// 删除贴源库
	@ResponseBody
	@RequestMapping("/removeCDW")
	public ResultBody removeCDW(HttpServletRequest request) {
		try {
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
			String dataCategoryId = request.getParameter("dataCategoryId"); // 分类Id
			String guid = request.getParameter("guid");
			String sourceType = request.getParameter("sourceType");// 贴源库来源类型
			String sql = "select * from bi_t_dbinfo where centrewarehouseid = '" + guid + "'";
			Map<String, Object> t_dbinfo = sqlMapperService.selectOne(sql);
			JSONObject jsonObj = JSONObject.fromObject(t_dbinfo.get("DBINFO"));
			DBInfoBean dbinfo = new DBInfoBean();
			dbinfo.HOSTNAME = jsonObj.getString("hostName");
			dbinfo.PORTNUMBER = jsonObj.getString("portNumber");
			dbinfo.DBTYPE = jsonObj.getString("DBType");
			if ("ORACLE".equals(jsonObj.getString("DBType"))) {
				dbinfo.SERVICENAME = jsonObj.getString("serviceName");
			}
			if (!DBTypeConstant.MPPDB.equals(jsonObj.getString("DBType"))) {
				dbinfo.DBUID = jsonObj.getString("DBUid");
				dbinfo.DBPWD = jsonObj.getString("DBPwd");
			}
			String cName = sqlMapperService.selectOne("select CNAME from RAW_centrewarehouse where guid='" + guid + "'", String.class);
			// 删除ETL对应的目录
			String rawDirectories = this.getRawDirectories(dataCategoryId);
			packageService.deleteDirByPath(user.optAdmdivCode, rawDirectories + "/" + cName);
			List<String> dropSqls = new ArrayList();
			if (!"02".equals(sourceType)) { // 贴源库来源类型 01:普通创建  02:注册  03：复制
				if ("ORACLE".equals(dbinfo.DBTYPE)) {
					String dropTables = "select 'drop table '||table_name as dropsql from cat where table_type='TABLE' AND substr(table_name,0,4) <> 'BIN$' AND table_name <> 'GID'";
					List<Map<String, Object>> dropTablesSqls = dynamicConnServiceV2.selectList(dbinfo, dropTables);
					if (dropTablesSqls != null) {
						for (Map<String, Object> m : dropTablesSqls) {
							dropSqls.add(m.get("DROPSQL").toString());
						}
						dynamicConnServiceV2.execDdls2(dbinfo, dropSqls);
					}
				}
			} else {
				List<String> dropCol = new ArrayList<>();
				List<Map<String, Object>> tableNames = sqlMapperService.selectList(" SELECT tablename from RAW_CWTABLE WHERE centrewarehouseid='" + guid + "'");
				for (Map<String, Object> map : tableNames) {
					if ("ORACLE".equals(dbinfo.DBTYPE)) {
						dynamicConnServiceV2.execDdl(dbinfo, "DECLARE \n num NUMBER; \n BEGIN \n SELECT COUNT(1) INTO num from cols" + " where table_name = upper('" + map.get("TABLENAME")
								+ "') and column_name = upper('DWTAB_CREATE_TIME'); \n IF num > 0 THEN \n" + " execute immediate 'alter table " + map.get("TABLENAME")
								+ " DROP (DWTAB_CREATE_TIME)'; \n END IF; \n " + "  SELECT COUNT(1) INTO num from cols" + " where table_name = upper('" + map.get("TABLENAME")
								+ "') and column_name = upper('DWTAB_EXPIRATION_TIME'); \n IF num > 0 THEN \n" + " execute immediate 'alter table " + map.get("TABLENAME")
								+ " DROP (DWTAB_EXPIRATION_TIME)'; \n END IF; \n END;");
					} else {
						dropCol.add("alter table " + map.get("TABLENAME") + " drop DWTAB_CREATE_TIME");
						dropCol.add("alter table " + map.get("TABLENAME") + " drop DWTAB_EXPIRATION_TIME");
					}
				}
				dynamicConnServiceV2.execDdls2(dbinfo, dropCol);
			}
			/*
			 * if(DBTypeConstant.MPPDB.equals(dbinfo.DBTYPE)){ List<Map<String,
			 * Object>> TableList = dynamicConnServiceV2.selectList(dbinfo,
			 * "show tables"); if(TableList != null){ for (Map<String, Object>
			 * map : TableList) { //list.add("drop table "+map.get("name")); }
			 * dynamicConnServiceV2.execDdls2(dbinfo, list); } }
			 */
			/*
			 * if("ORACLE".equals(jsonObj.getString("DBType")))
			 * dynamicConnServiceV2.execSql(dbinfo,
			 * "drop database "+dbinfo.DBUID); else
			 * if("MYSQL".equals(jsonObj.getString("DBType")))
			 * dynamicConnServiceV2.execSql(dbinfo,
			 * "drop database "+dbinfo.DBNAME);
			 */
			List<String> sqls = new ArrayList<String>();
			// 删除表中字段定义
			sqls.add(" DELETE RAW_cwtfactor WHERE tableid IN(SELECT GUID from RAW_CWTABLE WHERE centrewarehouseid='" + guid + "')");
			// 删除仓库中的表定义
			sqls.add(" DELETE RAW_CWTABLE WHERE centrewarehouseid='" + guid + "'");
			// 删除贴源库表分类
			sqls.add(" DELETE raw_cwtablecat t WHERE t.centrewarehouseid='" + guid + "'");
			// 删除库定义
			sqls.add(" DELETE RAW_centrewarehouse WHERE guid='" + guid + "'");

			// 删除目标管道信息
			sqls.add(" DELETE bi_t_dbinfo WHERE centrewarehouseid='" + guid + "'");
			// 删除采集方式，对应表关系
			// 数据库直采连接表
			sqls.add(" DELETE raw_etl_dbconnection WHERE rawid='" + guid + "'");
			// 是否支持手工录入
			sqls.add(" DELETE raw_etl_handinput WHERE rawid='" + guid + "'");
			// 采集方式对应表信息
			sqls.add(" DELETE raw_etl_tableconfig WHERE rawid='" + guid + "'");
			// 数据接口
			sqls.add(" DELETE raw_etl_interface WHERE rawid='" + guid + "'");
			// 变量表
			sqls.add(" delete from RAW_AUDITVAR a where exists (select b.id from BI_AUDITPOLICY_CATE b where rawguid = '" + guid + "' and a.policycateid = b.id)");
			// 审核规则表
			sqls.add(" delete from BI_AUDITPOLICY a where exists (select b.id from BI_AUDITPOLICY_CATE b where rawguid = '" + guid + "' and a.policycateid = b.id)");
			// 审核规则分类
			sqls.add(" delete from BI_AUDITPOLICY_CATE where rawguid ='" + guid + "'");
			// 审核结果表
			// sqls.add(" DELETE from RAW_PROGR_RESULT where exists(select guid from raw_auditprogramme b where b.rawguid='"+guid+"' and a.progrid = b.guid)");
			// 规则方案表
			sqls.add(" DELETE from raw_policy_progr a WHERE exists(select guid from raw_auditprogramme b where b.rawguid='" + guid + "' and a.progrid = b.guid)");
			// 审核方案表
			sqls.add(" DELETE raw_auditprogramme WHERE rawguid='" + guid + "'");
			// 删除转换信息中间表
			sqls.add(" DELETE bi_t_etltransinfo t WHERE t.busistoreid='"+ guid +"'");

			sqlMapperService.execSqls(sqls);
			mfbicommonService.deleteDLSInfo_database(MfbiCommon.APPID_RAW, guid);
			return ResultBody.createSuccessResult("操作成功！");
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("操作失败！");
		}
	}

	@ResponseBody
	@RequestMapping("/removeCDWDb")
	public ResultBody removeCDWDb(HttpServletRequest request) {
		try {
			String guid = request.getParameter("guid");
			List<String> sqls = new ArrayList<String>();
			String sql = "select * from bi_subject where datapipelineid = '" + guid + "'";
			SubjectBean subject = sqlMapperService.selectOne(sql, SubjectBean.class);
			DBInfoBean dbinfo = Common.getDBInfoBean(subject);
			dynamicConnServiceV2.execSql(dbinfo, "drop database " + dbinfo.DBNAME);
			return ResultBody.createSuccessResult("操作成功！");
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("操作失败！");
		}
	}

	/***************************** 库end *****************************************/

	/***************************** 表 ********************************************/

	/**
	 * 贴源库(RAW) table list
	 * 
	 * @param request
	 * @param centreWarehouseId
	 *            库id
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/cdwTableList")
	public List<Map<String, Object>> cdwTableList(HttpServletRequest request, String centreWarehouseId) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			String sql = "SELECT T.GUID AS ID, T.TABLECNAME  || '【' || T.TABLENAME || '】' AS NAME," + " (case when CATID is null then 'default' else CATID end) AS PID, "
					+ " T.TABLENAME,T.TABLECNAME,SOURCETABNAME, SOURCETABCNAME, SOURCETABTYPE,DBINFOID,0 as ORDERNUM ,ANNOTATION,t.is_view,t.REGTABLECHG,DECODE(t.is_view,'1','../a.png','') icon, T.EXPIRATION_UNIT, T.SAVEHISTORYDATA, T.DWTAB_EXPIRATION_TIME" + " from RAW_CWTABLE T where T.centreWarehouseId='"
					+ centreWarehouseId + "' " + " union " + " select C.GUID as ID, C.NAME,C.SUPERGUID as PID,'' as TABLENAME, '' as TABLECNAME,"
					+ " '' as SOURCETABNAME,'' as SOURCETABCNAME,'' as SOURCETABTYPE,'' as DBINFOID,C.ORDERNUM ,'' AS ANNOTATION,'' is_view,'' REGTABLECHG,'' icon , '' as EXPIRATION_UNIT, '' as SAVEHISTORYDATA, null as DWTAB_EXPIRATION_TIME" + " from RAW_CWTABLECAT C where C.CENTREWAREHOUSEID='"
					+ centreWarehouseId + "'" + " order by is_view ,NAME";
			List<Map<String, Object>> listData = sqlMapperService.selectList(sql);
			if (listData != null) {
				list.addAll(listData);
			}
			return list;
		} catch (Exception e) {
			e.printStackTrace();
			return list;
		}
	}

	/**
	 * 中心数据仓库(CDW) table list
	 * 
	 * @param request
	 * @param centreWarehouseId
	 *            库id 只列出存在轉換的表和分類
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/cdwTableListETL")
	public List<Map<String, Object>> cdwTableListETL(HttpServletRequest request, String centreWarehouseId) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			/*
			 * Map<String, Object> map = new HashMap<String,Object>();
			 * map.put("ID", "ROOT"); map.put("NAME", "所有表"); map.put("PID",
			 * null); list.add(map);
			 */

			String sql = "SELECT T.GUID AS ID,T.TABLECNAME || '【' || T.TABLENAME || '】' AS NAME,(case when CATID is null then'default' else CATID end ) AS PID,T.TABLENAME,T.TABLECNAME,SOURCETABNAME,SOURCETABCNAME,SOURCETABTYPE,DBINFOID,0 as ORDERNUM,ANNOTATION from RAW_CWTABLE T where T.centreWarehouseId ="
					+ "'"
					+ centreWarehouseId
					+ "'"
					+ " and exists (select 1 from bi_t_etltransinfo a where a.busiobjid = t.guid and  a.busitype = '01'and a.busistoreid ="
					+ "'"
					+ centreWarehouseId
					+ "'"
					+ "and a.etltype = '1')union all select C.GUID as ID,C.NAME,C.SUPERGUID as PID,'' as TABLENAME,'' as TABLECNAME,'' as SOURCETABNAME,'' as SOURCETABCNAME,'' as SOURCETABTYPE,'' as DBINFOID,C.ORDERNUM,'' AS ANNOTATION from RAW_CWTABLECAT C where C.CENTREWAREHOUSEID = "
					+ "'"
					+ centreWarehouseId
					+ "'"
					+ " and exists(SELECT 1 from RAW_CWTABLE T where T.centreWarehouseId = "
					+ "'"
					+ centreWarehouseId
					+ "'"
					+ " and exists (select 1 from bi_t_etltransinfo a where a.busiobjid = t.guid and a.busitype = '01'and a.busistoreid ="
					+ "'"
					+ centreWarehouseId
					+ "'"
					+ "and a.etltype = '1')and t.catid = c.guid)order by NAME";
			List<Map<String, Object>> listData = sqlMapperService.selectList(sql);
			if (listData != null) {
				list.addAll(listData);
			}
			return list;
		} catch (Exception e) {
			e.printStackTrace();
			return list;
		}
	}

	/**
	 * 中心数据仓库(CDW) table list 根据手工录入做为过滤条件
	 * 
	 * @param request
	 * @param centreWarehouseId
	 *            库id
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/cdwTableListByCollType")
	public List<Map<String, Object>> cdwTableListByCollType(HttpServletRequest request, String centreWarehouseId, String type) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			/*
			 * Map<String, Object> map = new HashMap<String,Object>();
			 * map.put("ID", "ROOT"); map.put("NAME", "所有表"); map.put("PID",
			 * null); list.add(map);
			 */
			String sql = "SELECT T.GUID AS ID, T.TABLECNAME  || '【' || T.TABLENAME || '】' AS NAME," + " (case when CATID is null then 'default' else CATID end) AS PID, "
					+ " T.TABLENAME,T.TABLECNAME,SOURCETABNAME, SOURCETABCNAME, SOURCETABTYPE,DBINFOID,0 as ORDERNUM ,ANNOTATION"
					+ " from RAW_CWTABLE T,raw_etl_tableconfig t1 , raw_etl_handinput t2 where T.centreWarehouseId='" + centreWarehouseId + "' and t1.collecttype='02' and t.guid = t1.tableid "
					+ " and t2.rawid = '" + centreWarehouseId + "' and t2.issupporthand = '1' and t2.rawid = t.centreWarehouseId" + " union "
					+ " select C.GUID as ID, C.NAME,C.SUPERGUID as PID,'' as TABLENAME, '' as TABLECNAME,"
					+ " '' as SOURCETABNAME,'' as SOURCETABCNAME,'' as SOURCETABTYPE,'' as DBINFOID,C.ORDERNUM ,'' AS ANNOTATION" + " from RAW_CWTABLECAT C where C.CENTREWAREHOUSEID='"
					+ centreWarehouseId + "'" + " order by NAME";
			List<Map<String, Object>> listData = sqlMapperService.selectList(sql);
			if (listData != null) {
				list.addAll(listData);
			}
			return list;
		} catch (Exception e) {
			e.printStackTrace();
			return list;
		}
	}

	/**
	 * 中心数据仓库(CDW) table detail
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getCDWTable")
	public Map<String, Object> getCDWTable(HttpServletRequest request, String guid) {
		try {
			return sqlMapperService.selectOne("select * from RAW_CWTABLE where guid='" + guid + "'");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/***
	 * 增加或修改贴源库“表”定义
	 * 
	 * @param request
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/addOrUpdateCDWTable")
	@Description("增加或修改贴源库表定义")
	public ResultBody addOrUpdateCDWTable(HttpServletRequest request) {
		UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
		String guid = request.getParameter("guid");
		String tableName = request.getParameter("tableName"); // 表物理名称
		String tableCName = request.getParameter("tableCName"); // 表中文名称
		String type = request.getParameter("type"); // 是否添加操作
		String DWTAB_EXPIRATION_TIME = request.getParameter("DWTAB_EXPIRATION_TIME"); // 过期时间
		String unitVal = request.getParameter("unitVal"); // 数据过期时间单位
		String saveHistoryData = request.getParameter("SAVEHISTORYDATA"); // 数据过期时间单位
		String catId = request.getParameter("catId"); // 数据过期时间单位
		String centreWarehouseId = request.getParameter("centreWarehouseId"); // 所属库
		String remark = request.getParameter("remark"); // 备注
		Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId); // 仓库目标管道
		if (targetPipe == null) {
			return ResultBody.createErrorResult("目标数据管道信息获取失败，请检查！");
		}

		DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());
		try {
			dynamicConnServiceV2.testConn(dbinfo);
		} catch (Exception e1) {
			return ResultBody.createErrorResult("目标数据管道连接失败 </br>" + e1.getMessage());
		}
		try {

			String targetPipeId = targetPipe.get("GUID").toString(); // 目标管道ID
			String sql = "", guid_new = "";
			String editKey = request.getParameter("editKey");
			// 表描述信息
			if (guid != null) {
				guid_new = guid;
				sql = " UPDATE RAW_CWTABLE SET tablename = '" + tableName + "', tablecname = '" + tableCName + "', centrewarehouseid = '" + centreWarehouseId + "',ANNOTATION='"
						+ ("".equals(remark) ? "" : remark) + "',ALTERUSER='" + user.UserName + "',ALTERUSERID='" + user.MID + "',UPDATETIME=sysdate,DWTAB_EXPIRATION_TIME='"+DWTAB_EXPIRATION_TIME+"',EXPIRATION_UNIT='"+unitVal+"',SAVEHISTORYDATA='"+saveHistoryData+"' WHERE GUID='" + guid + "'";
			} else {
				// 新增表
				guid_new = sqlMapperService.selectOne("select sys_guid() as guid from dual", String.class);
				sql = " INSERT INTO RAW_CWTABLE (guid,tablename, tablecname, centrewarehouseid,createdate,SOURCETABTYPE,DBINFOID,CREATER,DWTAB_EXPIRATION_TIME,EXPIRATION_UNIT,SAVEHISTORYDATA,CATID,ANNOTATION,CREATEUSER,CREATEUSERID) "
						+ "VALUES('"
						+ guid_new
						+ "', '"
						+ tableName
						+ "', '"
						+ tableCName
						+ "', '"
						+ centreWarehouseId
						+ "',sysdate,'01', '"
						+ targetPipeId
						+ "','"
						+ user.UserName
						+ "',"
						+ DWTAB_EXPIRATION_TIME
						+ ",'" + unitVal + "','" + saveHistoryData + "','" + catId + "','" + ("".equals(remark) ? "" : remark) + "','" + user.UserName + "','" + user.MID + "')";
			}

			List<String> sqls = new ArrayList<String>();
			sqls.add(sql);
			if ("add".equalsIgnoreCase(type)) {

				String factorInfoRows = request.getParameter("factorInfoRows"); // 增加或修改表的所有列信息（rows）
				// 列信息维护
				@SuppressWarnings("unchecked")
				List<Map<String, Object>> facts = JSONArray.fromObject(factorInfoRows);
				for (Map<String, Object> map : facts) {
					sqls.add("INSERT INTO RAW_cwtfactor (ISPK,COLUMNNAME,COLUMNCNAME, DATATYPE,DATA_LENGTH,"
							+ "DATA_SCALE, NOTNULL,DATA_DEFAULT, ANNOTATION, TABLEID,ISPARTITION,PARTITION_ORDER,ORDERNUM,INPUTRULES,CREATEUSER,CREATEUSERID) " + "VALUES('"
							+ map.get("ISPK")
							+ "','"
							+ map.get("COLUMNNAME")
							+ "','"
							+ (map.get("COLUMNCNAME") == null ? "" : map.get("COLUMNCNAME"))
							+ "', '"
							+ map.get("DATATYPE")
							+ "', "
							+ ((map.get("DATA_LENGTH") != null && !map.get("DATA_LENGTH").equals("")) ? map.get("DATA_LENGTH") : null)
							+ ", '"
							+ (map.get("DATA_SCALE") == null ? "" : map.get("DATA_SCALE"))
							+ "', '"
							+ (map.get("NOTNULL") == null ? "" : map.get("NOTNULL"))
							+ "','"
							+ (map.get("DATA_DEFAULT") == null ? "" : map.get("DATA_DEFAULT"))
							+ "','"
							+ (map.get("ANNOTATION") == null ? "" : map.get("ANNOTATION"))
							+ "', '"
							+ guid_new
							+ "','"
							+ (map.get("ISPARTITION") == null ? "0" : map.get("ISPARTITION"))
							+ "','"
							+ (map.get("PARTITION_ORDER") == null ? "" : map.get("PARTITION_ORDER"))
							+ "','"
							+ (map.get("ORDERNUM") == null ? "" : map.get("ORDERNUM"))
							+ "','"
							+ (map.get("INPUTRULES") == null ? "" : map.get("INPUTRULES"))
							+ "','"
							+ user.UserName
							+ "','"
							+ user.MID + "')");
				}
				// 构建物理表
				ResultBody createCDWTable = this.createCDWTable(dbinfo, tableName, tableCName, facts, editKey);
				if (createCDWTable.isError) {
					if (DBTypeConstant.ORACLE.equals(dbinfo.DBTYPE)) {
						this.dropCDWTable(dbinfo, tableName);
					}
					return createCDWTable;

				}
				/*
				 * String typeSql =
				 * "select sourcetype from RAW_centrewarehouse where guid = '"
				 * +centreWarehouseId+"'"; Map<String, Object> source =
				 * sqlMapperService.selectOne(typeSql);
				 * if(!"02".equals(source.get("SOURCETYPE"))){ //构建物理表
				 * ResultBody createCDWTable =
				 * this.createCDWTable(dbinfo,tableName
				 * ,tableCName,facts,editKey); if(createCDWTable.isError) return
				 * createCDWTable; }
				 */

				// 贴源库表新增 , 建立初始转换
				ResultBody result_createTrans = mfbicommonService.createEmptyTrans("1", centreWarehouseId, tableCName, tableName, user.optAdmdivCode, "");
				if (result_createTrans.isError) {
					this.dropCDWTable(dbinfo, tableName);
					return result_createTrans;
				}
				Map<String, Object> _map = (Map<String, Object>) result_createTrans.result;
				String transId = _map.get("transId").toString();// 转换id
				// 保存转换信息
				mfbicommonService.create_eltransinfo(user.optAdmdivCode, Integer.parseInt(transId), centreWarehouseId, guid_new, "01", "1", user.UserName, "0");
				// 贴源库表定义存储转换id
				String updateCDW = "update RAW_CWTABLE set TRANSID='" + transId + "' where GUID='" + guid_new + "'";
				sqls.add(updateCDW);
				String guid_main = sqlMapperService.selectOne("select sys_guid() from dual", String.class);
				// 将信息保存到贴源库信息主表
				/*
				 * String sql_RawMain=
				 * "insert into RAW_SOURCE_INFOMAIN (GUID,SOURCEPIPEID,EXTRACTTYPE,COLLECTTYPE,RAWTABLEGUID,RAWGUID,TRANSID)"
				 * +
				 * "values('"+guid_main+"','"+targetPipeId+"','0', '0','"+guid_new
				 * +"','"+centreWarehouseId+"','"+transId+"')";
				 * sqls.add(sql_RawMain);
				 */

			} else {
				mfbicommonService.updateDLSInfo_table(MfbiCommon.APPID_RAW, centreWarehouseId, tableName, tableCName);
			}

			// 修改贴源库定义
			sqlMapperService.execSqls2(sqls);

			Map<String, Object> resMap = new HashMap<String, Object>();
			resMap.put("guid", guid_new);
			resMap.put("msg", guid != null ? "表信息更新成功！" : (tableCName + "【" + tableName + "】</br>创建成功！"));
			return ResultBody.createSuccessResult(resMap);
		} catch (Exception e) {
			e.printStackTrace();
			this.dropCDWTable(dbinfo, tableName);
			return ResultBody.createErrorResult("操作失败！");
		}
	}

	/***
	 * 增加或修改贴源库物理表字段和表定义
	 * 
	 * @param request
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/addOrUpdatePhysicalCDWTable")
	@Description("增加或修改贴源库物理表字段和表定义")
	public ResultBody addOrUpdatePhysicalCDWTable(HttpServletRequest request) {
		try {
			String centreWarehouseId = request.getParameter("centreWarehouseId"); // 所属库
			Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId); // 仓库目标管道
			if (targetPipe == null) {
				return ResultBody.createErrorResult("目标数据管道信息获取失败，请检查！");
			}

			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());
			boolean testConn = dynamicConnServiceV2.testConn(dbinfo);
			if (!testConn) {
				return ResultBody.createErrorResult("目标数据管道连接失败，请检查！");
			}

			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
			String guid = request.getParameter("guid");
			String tableName = request.getParameter("tableName"); // 表物理名称
			String tableCName = request.getParameter("tableCName"); // 表中文名称
			String factorInfoRows = request.getParameter("factorInfoRows"); // 增加或修改表的所有列信息（rows）
			String curRows = request.getParameter("curRow"); // 当前增加或修改的列信息（rows）
			String editKey = request.getParameter("editKey");
			String sourceType = request.getParameter("sourceType");
			List<String> sqls = new ArrayList<String>();
			String type = request.getParameter("type");// 获取当前列的新增或修改状态
			// sqls.add("delete RAW_cwtfactor where TABLEID='"+guid+"'");
			// //先删除当前表的列信息
			JSONObject curRow = JSONObject.fromObject(curRows);
			if ("add".equals(type)) {
				sqls.add("INSERT INTO RAW_cwtfactor (ISPK,COLUMNNAME,COLUMNCNAME, DATATYPE,DATA_LENGTH,"
						+ "DATA_SCALE, NOTNULL,DATA_DEFAULT, ANNOTATION, TABLEID,ISPARTITION,PARTITION_ORDER,ORDERNUM,INPUTRULES,CREATEUSER,CREATEUSERID) " + "VALUES('"
						+ curRow.get("ISPK")
						+ "','"
						+ curRow.get("COLUMNNAME")
						+ "','"
						+ (curRow.get("COLUMNCNAME") == null ? "" : curRow.get("COLUMNCNAME"))
						+ "', '"
						+ curRow.get("DATATYPE")
						+ "', "
						+ ((curRow.get("DATA_LENGTH") != null && !curRow.get("DATA_LENGTH").equals("")) ? curRow.get("DATA_LENGTH") : null)
						+ ", '"
						+ (curRow.get("DATA_SCALE") == null ? "" : curRow.get("DATA_SCALE"))
						+ "', '"
						+ (curRow.get("NOTNULL") == null ? "" : curRow.get("NOTNULL"))
						+ "','"
						+ (curRow.get("DATA_DEFAULT") == null ? "" : curRow.get("DATA_DEFAULT"))
						+ "','"
						+ (curRow.get("ANNOTATION") == null ? "" : curRow.get("ANNOTATION"))
						+ "', '"
						+ guid
						+ "','"
						+ (curRow.get("ISPARTITION") == null ? "0" : curRow.get("ISPARTITION"))
						+ "','"
						+ (curRow.get("PARTITION_ORDER") == null ? "" : curRow.get("PARTITION_ORDER"))
						+ "','"
						+ (curRow.get("ORDERNUM") == null ? "" : curRow.get("ORDERNUM"))
						+ "','"
						+ (curRow.get("INPUTRULES") == null ? "" : curRow.get("INPUTRULES"))
						+ "','"
						+ user.UserName
						+ "','"
						+ user.MID + "')");
			} else if ("edit".equals(type)) {
				String colid = request.getParameter("colId");
				String sql = "update RAW_cwtfactor set ISPK = '" + curRow.get("ISPK") + "',COLUMNNAME='" + curRow.get("COLUMNNAME") + "'," + "COLUMNCNAME = '" + curRow.get("COLUMNCNAME")
						+ "',DATATYPE='" + curRow.get("DATATYPE") + "',DATA_LENGTH="
						+ ((curRow.get("DATA_LENGTH") != null && !curRow.get("DATA_LENGTH").equals("")) ? curRow.get("DATA_LENGTH") : null) + "," + "DATA_SCALE="
						+ ((curRow.get("DATA_SCALE") != null && !curRow.get("DATA_SCALE").equals("")) ? curRow.get("DATA_SCALE") : null) + ",NOTNULL='"
						+ (curRow.get("NOTNULL") == null ? "" : curRow.get("NOTNULL")) + "'," + "DATA_DEFAULT = '" + (curRow.get("DATA_DEFAULT") == null ? "" : curRow.get("DATA_DEFAULT"))
						+ "',ANNOTATION='" + (curRow.get("ANNOTATION") == null ? "" : curRow.get("ANNOTATION")) + "'," + "TABLEID='" + guid + "',ISPARTITION = '"
						+ (curRow.get("ISPARTITION") == null ? "0" : curRow.get("ISPARTITION")) + "'," + "PARTITION_ORDER="
						+ ((curRow.get("PARTITION_ORDER") != null && !curRow.get("PARTITION_ORDER").equals("")) ? curRow.get("PARTITION_ORDER") : null) + "," + "ORDERNUM='"
						+ (curRow.get("ORDERNUM") == null ? "" : curRow.get("ORDERNUM")) + "',INPUTRULES='" + (curRow.get("INPUTRULES") == null ? "" : curRow.get("INPUTRULES")) + "'," + "ALTERUSER='"
						+ user.UserName + "',ALTERUSERID='" + user.MID + "',UPDATETIME=sysdate," + "DATA_DEFAULT_OLD = '"
						+ (curRow.get("DATA_DEFAULT_OLD") == null ? "" : curRow.get("DATA_DEFAULT_OLD")) + "' where guid='" + colid + "'";
				sqls.add(sql);
			}
			/*
			 * //列信息维护 for (Map<String, Object> map : facts){ sqls.add(
			 * "INSERT INTO RAW_cwtfactor (ISPK,COLUMNNAME,COLUMNCNAME, DATATYPE,DATA_LENGTH,"
			 * +
			 * "DATA_SCALE, NOTNULL,DATA_DEFAULT, ANNOTATION, TABLEID,ISPARTITION,PARTITION_ORDER,ORDERNUM,INPUTRULES,CREATEUSER,CREATEUSERID) "
			 * + "VALUES('"+map.get("ISPK")+"','"+map.get("COLUMNNAME")+"','"
			 * +(map.get("COLUMNCNAME") == null ? "" :
			 * map.get("COLUMNCNAME"))+"', '" + map.get("DATATYPE")+"', "
			 * +((map.get("DATA_LENGTH") != null &&
			 * !map.get("DATA_LENGTH").equals("")) ? map.get("DATA_LENGTH") :
			 * null)+", '"
			 * +(map.get("DATA_SCALE")==null?"":map.get("DATA_SCALE"))+"', '"
			 * +(map.get("NOTNULL")==null?"":map.get("NOTNULL"))+"','"
			 * +(map.get("DATA_DEFAULT")==null?"":map.get("DATA_DEFAULT"))+"','"
			 * +(map.get("ANNOTATION")==null?"":map.get("ANNOTATION"))+"', '"
			 * +guid+"','"+(map.get("ISPARTITION") == null ? "0" :
			 * map.get("ISPARTITION"))+"','" +(map.get("PARTITION_ORDER") ==
			 * null ? "":map.get("PARTITION_ORDER"))+"','" +(map.get("ORDERNUM")
			 * == null ? "":map.get("ORDERNUM"))+"','" +(map.get("INPUTRULES")
			 * == null ?
			 * "":map.get("INPUTRULES"))+"','"+user.UserName+"','"+user
			 * .MID+"')"); }
			 */
			JSONArray facts = JSONArray.fromObject(factorInfoRows);
			String IS_VIEW = sqlMapperService.selectOne("select IS_VIEW from RAW_cwtable where guid = '" + guid + "'", String.class);
			/* Map<String, Object> map = sqlMapperService.selectOne(typeSql); */
			if (!"02".equals(sourceType) && !"1".equals(IS_VIEW)) {
				// 构建物理表
				ResultBody createCDWTable = this.createCDWTable(dbinfo, tableName, tableCName, facts, editKey);
				if (createCDWTable.isError) {
					return createCDWTable;
				}
			}
			// 构建物理表
			// ResultBody createCDWTable =
			// this.createCDWTable(dbinfo,tableName,tableCName,facts,editKey);
			// if(createCDWTable.isError)
			// return createCDWTable;
			// 修改贴源库定义
			sqls.add("update raw_cwtable set ALTERUSER = '" + user.UserName + "',ALTERUSERID='" + user.MID + "',UPDATETIME=sysdate where guid='" + guid + "'");
			sqlMapperService.execSqls2(sqls);

			Map<String, Object> resMap = new HashMap<String, Object>();
			resMap.put("guid", guid);
			resMap.put("msg", "表信息更新成功！");
			return ResultBody.createSuccessResult(resMap);
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("操作失败！");
		}
	}

	/**
	 * 根据贴源库仓库id获取目标管道
	 * 
	 * @param centreWarehouseId
	 */
	private Map<String, Object> getTargetPipe(String centreWarehouseId) {
		return sqlMapperService.selectOne("select t1.*, t2.pipelinename as pipelinename_real from BI_T_DBINFO t1, bi_datapipeline t2 where t1.pipelineid = t2.guid and centreWarehouseId='"
				+ centreWarehouseId + "' ");
	}

	@ResponseBody
	@RequestMapping("/getTargetPipeByCWId")
	public Map<String, Object> getTargetPipeByCWId(String centreWarehouseId) {
		return getTargetPipe(centreWarehouseId);
	}

	@ResponseBody
	@RequestMapping("/getData")
	public boolean getData(String dbType, String tableName, String centreWarehouseId) {
		Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId);
		DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(dbType, targetPipe.get("DBINFO").toString());
		String sql = "SELECT TABLENAME FROM RAW_CWTABLE WHERE centreWarehouseId='" + centreWarehouseId + "' and UPPER(tablename) = '" + tableName.toUpperCase() + "'";
		String tabName = sqlMapperService.selectOne(sql, String.class);
		String sql_list = "select COUNT(1) from " + tabName;
		Integer count = dynamicConnServiceV2.selectOne(dbinfo, sql_list, Integer.class);
		if (count != null && count > 0) {
			return false;
		} else {
			return true;
		}
	}

	@ResponseBody
	@RequestMapping("/getTargetPipeDBTypeByCWId")
	public Map<String, Object> getTargetPipeDBTypeByCWId(String centreWarehouseId) {
		Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId);
		Map<String, Object> DBType = new HashMap<String, Object>();
		if (targetPipe != null) {
			DBType.put("DBTYPE", targetPipe.get("DBTYPE"));
		}
		return DBType;
	}

	/**
	 * 根据管道id获取管道信息
	 * 
	 * @param sourcepipeid
	 * @return
	 */
	@ResponseBody
	@RequestMapping("getPipeInfoByGuid")
	public ResultBody getPipeInfoByGuid(String sourcepipeid) {
		String sqlString = "select * from BI_DATAPIPELINE where guid = '" + sourcepipeid + "'";
		Map<String, Object> pipeInfoMap = sqlMapperService.selectOne(sqlString);
		return ResultBody.createSuccessResult(pipeInfoMap);
	}

	/**
	 * 数据类型
	 * 
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getDataTypeList")
	public List<Map<String, Object>> getDataTypeList() {
		try {
			String sql = "SELECT CODEN,CNAME from bi_code WHERE basename='DATATYPE'";
			return sqlMapperService.selectList(sql);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * zmx 删除贴源库 表
	 * 
	 * @param guid
	 * @param dbTabName
	 * @param dbInfoId
	 * @param sourceType
	 *            贴源库来源类型，注册的贴源库不操作物理定义，只删除逻辑定义 sourceType：02 为注册贴源库
	 * @param centreWarehouseId
	 *            贴源库id
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/removeCDWTable")
	@Description("删除贴源库表 ")
	public ResultBody removeCDWTable(HttpServletRequest request, String guid, String dbTabName, String dbInfoId, String sourceType, String centreWarehouseId) {
		try {
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");

			String sql = "select * from bi_t_dbinfo where GUID = '" + dbInfoId + "'";
			// 数据管道
			Map<String, Object> pipeline = sqlMapperService.selectOne(sql);
			if (pipeline == null) {
				return ResultBody.createErrorResult("数据管道信息获取失败，请检查！");
			}

			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
			boolean testConn = dynamicConnServiceV2.testConn(dbinfo);
			if (!testConn) {
				return ResultBody.createErrorResult("当前数据管道连接失败，请检查！");
			}
			List<String> sqls = new ArrayList<String>();
			Map<String, Object> map = sqlMapperService.selectOne("select SOURCETABTYPE,TRANSID,IS_VIEW from RAW_CWTABLE where guid='" + guid + "'");
			if ((sourceType != null && !"1".equals(map.get("IS_VIEW")) && !("02".equals(sourceType))) || "01".equals(map.get("SOURCETABTYPE"))) {
				// 删除物理表定义
				ArrayList<String> dropTableSqlList = new ArrayList<>();
				dropTableSqlList.add("DROP TABLE " + dbTabName);
				dropTableSqlList.add("DROP TABLE " + dbTabName + "_b");
				dynamicConnServiceV2.execDdls2(dbinfo, dropTableSqlList);
				//dynamicConnServiceV2.execSql(dbinfo, "DROP TABLE " + dbTabName + "_b");
			} else if ("02".equals(sourceType) && !"1".equals(map.get("IS_VIEW"))) {
				if(DBTypeConstant.MYSQL.equals(dbinfo.DBTYPE)){
					List<Map<String, Object>> tabList = dynamicConnServiceV2.selectList(dbinfo,
							"select * from information_schema.columns where table_name = upper('ele_enterprise') and (column_name = upper('dwtab_create_time') or column_name = upper('DWTAB_EXPIRATION_TIME'))");
					List sqlList = new ArrayList();
					for( Map<String, Object> tabM : tabList){
						if(tabM.get("column_name").toString().equalsIgnoreCase("DWTAB_CREATE_TIME"))
							sqlList.add("alter table ELE_ENTERPRISE DROP column DWTAB_CREATE_TIME");
						else if(tabM.get("column_name").toString().equalsIgnoreCase("DWTAB_EXPIRATION_TIME"))
							sqlList.add("alter table ELE_ENTERPRISE DROP column DWTAB_EXPIRATION_TIME");
					}
					dynamicConnServiceV2.execSqls2(dbinfo,sqlList);
					/*dynamicConnServiceV2.execDdl(dbinfo,"drop procedure if exists schema_change;  \n" +
							"delimiter '&';  \n" +
							"create procedure schema_change() begin  \n" +
							"if exists (select * from information_schema.columns where table_name = upper('ele_enterprise') and column_name = upper('dwtab_create_time')) then  \n" +
							"alter table ELE_ENTERPRISE DROP column DWTAB_CREATE_TIME;  \n" +
							"end if;  \n" +
							"if exists (select * from information_schema.columns where table_name = upper('ele_enterprise') and column_name = upper('DWTAB_EXPIRATION_TIME')) then  \n" +
							"alter table ELE_ENTERPRISE DROP column DWTAB_EXPIRATION_TIME;  \n" +
							"end if;  \n" +
							"end;;  \n" +
							"delimiter ';';  \n" +
							"call schema_change();  \n" +
							"drop procedure if exists schema_change;");*/
				}else
					dynamicConnServiceV2.execDdl(dbinfo, "DECLARE \n num NUMBER; \n BEGIN \n SELECT COUNT(1) INTO num from cols" + " where table_name = upper('" + dbTabName
						+ "') and column_name = upper('DWTAB_CREATE_TIME'); \n IF num > 0 THEN \n" + " execute immediate 'alter table " + dbTabName + " DROP (DWTAB_CREATE_TIME)'; \n END IF; \n "
						+ "  SELECT COUNT(1) INTO num from cols" + " where table_name = upper('" + dbTabName + "') and column_name = upper('DWTAB_EXPIRATION_TIME'); \n IF num > 0 THEN \n"
						+ " execute immediate 'alter table " + dbTabName + " DROP (DWTAB_EXPIRATION_TIME)'; \n END IF; \n END;");
			}
			// 删除表中字段定义
			sqls.add("DELETE RAW_cwtfactor WHERE tableid='" + guid + "'");
			// 删除表定义
			sqls.add("DELETE RAW_CWTABLE WHERE guid='" + guid + "'");
			// 删除采集方式定义
			sqls.add("DELETE raw_etl_tableconfig where tableid='" + guid + "'");
			sqlMapperService.execSqls(sqls);
			// 获取表的转换id
			String transid = mfbicommonService.getTranIdsFromEtlCfg(centreWarehouseId, guid, "01");
			// 删除维度对应的转换
			// Object transid = map.get("TRANSID");//转换id
			if (transid != null && !"".equals(transid)) {
				ResultBody res = packageService.deleteTransByID(user.optAdmdivCode, transid.toString());
				if (res.isError) {
					return res;
				}
			}
			// 删除MFBI仓库表对应的转换信息（一表对多个转换的方式）
			mfbicommonService.deleteMfbiEtlCfgInfo(centreWarehouseId, "01", guid);
			// 删除血缘关系数据
			mfbicommonService.deleteDLSInfo_table(MfbiCommon.APPID_RAW, centreWarehouseId, dbTabName);
			return ResultBody.createSuccessResult("操作成功！");
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("操作失败！");
		}
	}

	/**
	 * 中心数据仓库(CDW) 表结构信息 list
	 * 
	 * @param request
	 * @param centreWarehouseId
	 *            库id
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/cwtFactorList")
	public List<Map<String, Object>> cdwFactorList(HttpServletRequest request, String tableId) {
		try {
			/*
			 * return sqlMapperService.selectList(
			 * "select guid,columnname,datatype,notnull,annotation,tableid,columncname,ispk,data_default,"
			 * +
			 * "data_scale,data_length,ispartition,partition_order,ordernum,inputrules,createuser,alteruser,to_char(updatetime,'yyyy-mm-dd HH24:MI:SS') as updatetime "
			 * + "from raw_cwtfactor where tableId='"+tableId+
			 * "' and COLUMNNAME NOT IN('DWTAB_EXPIRATION_TIME','DWTAB_CREATE_TIME') order by ORDERNUM,columnname"
			 * );
			 */
			return sqlMapperService.selectList("select t.*,to_char(t.UPDATETIME,'yyyy-MM-dd HH24:mi:ss') UPDATETIM_E_ from RAW_cwtfactor t where tableId='" + tableId
					+ "' and COLUMNNAME NOT IN('DWTAB_EXPIRATION_TIME','DWTAB_CREATE_TIME','DWTAB_LAST_UPDATE_TIME') order by ORDERNUM,columnname");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 中心数据仓库(CDW) 表结构信息 list
	 * 
	 * @param request
	 * @param centreWarehouseId
	 *            库id
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/cwtFactorList_dim")
	public List<Map<String, Object>> cwtFactorList_dim(HttpServletRequest request, String tableId) {
		try {
			return sqlMapperService.selectList("select * from RAW_cwtfactor where tableId='" + tableId
					+ "' and DATATYPE <> '大文本' and COLUMNNAME NOT IN('DWTAB_EXPIRATION_TIME','DWTAB_CREATE_TIME','DWTAB_DATACOLLECTION','DWTAB_LAST_UPDATE_TIME','BID') order by ordernum,columnname");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@ResponseBody
	@RequestMapping(value = "/checkPipeException")
	public ResultBody checkPipeException(String dataPipelineId, String _type) {
		return this.checkPipeException_(dataPipelineId, _type);
	}

	private ResultBody checkPipeException_(String dataPipelineId, String _type) {
		// 数据管道
		Map<String, Object> pipeline = this.getPipeline(dataPipelineId);
		if (pipeline == null) {
			return ResultBody.createErrorResult("数据管道信息获取失败，请检查！");
		}
		DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
		try {
			dynamicConnServiceV2.testConn(dbinfo);
		} catch (Exception e) {
			return ResultBody.createErrorResult("当前数据管道连接失败，无法获取码表信息！");
		}

		// 如果建表方式为从平台，则校验管道是否为平台管道
		if ("02".equals(_type)) {
			String sql = "SELECT d.tablecode FROM FASP_T_DICDS T, fasp_t_dictable d WHERE T.STATUS = '1' and t.tablecode = d.tablecode and rownum = 1";
			List<Map<String, Object>> dataList = dynamicConnServiceV2.selectList(dbinfo, sql);
			if (dataList == null) {
				return ResultBody.createErrorResult("当前使用的管道非平台管道，请检查管道是否正确！");
			}
		}

		return ResultBody.createSuccessResult(dbinfo);
	}

	/**
	 * 根据业务库批量建表之 查询业务库中的表和视图
	 * 
	 * @param request
	 * @param dataPipelineId
	 *            数据管道Id
	 * @param _type
	 *            建表方式 从业务库
	 * @return
	 */
	@ResponseBody
	@RequestMapping(value = "/getDBTablesAndViewsBy_busi", method = RequestMethod.POST)
	public PageBean<Map<String, Object>> getDBTablesAndViewsBy_busi(HttpServletRequest request, String dataPipelineId, String _type, String page, String rows, String onlyShowReg) {
		try {
			Map<String, Object> pipeline = this.getPipeline(dataPipelineId);
			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
			int start = 0, end = 0;
			if (page != null && rows != null) {
				int p = Integer.parseInt(page);
				int rs = Integer.parseInt(rows);
				start = (p - 1) * rs + 1;
				end = p * rs;
			}
			// 查询条件
			String search_str = request.getParameter("search_str");

			String sql = "select t.tablename table_name,t.tablecname table_cname,'TABLE' as table_type from  raw_cwtable t "
					+ " left join BI_T_DBINFO t1 on t1.centrewarehouseid=t.centrewarehouseid left join BI_DATAPIPELINE t2 on t2.guid = t1.pipelineid" + " where t2.guid='" + dataPipelineId + "'";
			if (search_str != null && !"".equals(search_str.trim())) {
				search_str = search_str.trim().toUpperCase();
				sql += " and (UPPER(t.tablename) like '%" + search_str + "%' or UPPER(t.tablecname) like '%" + search_str + "%') ";
			}
			sql += " ORDER BY t.tablename ";

			String sqlPage = "select * from (select rownum r,t.* from (" + sql + ") t " + " where rownum <=" + end + " ) where r >=" + start;
			// List<Map<String, Object>> dataList =
			// dynamicConnServiceV2.selectList(dbinfo, sqlPage);
			List<Map<String, Object>> dataList = sqlMapperService.selectList(sqlPage);

			isRegistRawByDataPipelineId(dataList, dataPipelineId, onlyShowReg);

			Integer count = sqlMapperService.selectOne("select count(t3.table_name) from (" + sql + ") t3", Integer.class);

			PageBean<Map<String, Object>> pageBean = new PageBean<Map<String, Object>>();
			pageBean.setRows(dataList);
			pageBean.setTotal(count == null ? 0 : count);
			return pageBean;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 根据业务库批量建表之 查询业务库中的表和视图
	 * 
	 * @param request
	 * @param dataPipelineId
	 *            数据管道Id
	 * @param _type
	 *            建表方式 02 ：从平台 03：从业务库
	 * @return
	 */
	@ResponseBody
	@RequestMapping(value = "/getDBTablesAndViews", method = RequestMethod.POST)
	public PageBean<Map<String, Object>> getDBTablesAndViews(HttpServletRequest request, String dataPipelineId, String _type, String page, String rows, String onlyShowReg) {
		try {
			Map<String, Object> pipeline = this.getPipeline(dataPipelineId);
			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
			int start = 0, end = 0;
			if (page != null && rows != null) {
				int p = Integer.parseInt(page);
				int rs = Integer.parseInt(rows);
				start = (p - 1) * rs + 1;
				end = p * rs;
			}
			// 查询条件
			String search_str = request.getParameter("search_str");
			String sql = "";
			if ("02".equals(_type)) { // 从平台查找
				sql = "SELECT d.tablecode as table_name,table_name as table_cname,d.dbtabname,d.name AS table_cname,d.year,'table' as table_type FROM FASP_T_DICDS T, fasp_t_dictable d WHERE T.STATUS = '1' and t.tablecode = d.tablecode ";
				if (search_str != null && !search_str.isEmpty()) {
					search_str = search_str.toUpperCase();
					sql += " and (UPPER(d.tablecode) like '%" + search_str + "%' or UPPER(d.name) like '%" + search_str + "%' or UPPER(d.year) like '%" + search_str + "%') ";
				}
				sql += " order by t.tablecode";
			} else { // 从业务库查找
				sql = "SELECT table_name,table_name as table_cname,table_type from user_tab_comments ";
				if (search_str != null && !"".equals(search_str.trim())) {
					search_str = search_str.trim().toUpperCase();
					sql += " where (UPPER(table_name) like '%" + search_str + "%' or UPPER(table_type) like '%" + search_str + "%') ";
				}
				sql += " ORDER BY table_type ,table_NAME ";
			}

			String sqlPage = "select * from (select rownum r,t.* from (" + sql + ") t " + " where rownum <=" + end + " ) where r >=" + start;

			List<Map<String, Object>> dataList = dynamicConnServiceV2.selectList(dbinfo, sqlPage);

			if ("03".equals(_type)) {
				isRegistRawByDataPipelineId(dataList, dataPipelineId, onlyShowReg);
			}
			Integer count = dynamicConnServiceV2.selectOne(dbinfo, "select count(t.table_name) from (" + sql + ") t", Integer.class);

			PageBean<Map<String, Object>> pageBean = new PageBean<Map<String, Object>>();
			pageBean.setRows(dataList);
			pageBean.setTotal(count == null ? 0 : count);
			return pageBean;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	// 从业务库批量建表功能中，如果当前业务库管道为注册的贴源库，取出该贴源库表信息
	private void isRegistRawByDataPipelineId(List<Map<String, Object>> dataList, String dataPipelineId, String onlyShowReg) {
		// List<Map<String, Object>> lastData = new ArrayList<Map<String,
		// Object>>();
		// 1.使用当前所选业务库管道反向查找该管道是否是注册贴源库
		String sql = "SELECT r.sourcetype,r.guid from raw_centrewarehouse r,bi_t_dbinfo t WHERE r.guid=t.centrewarehouseid AND t.pipelineid='" + dataPipelineId + "'";
		Map<String, Object> map = sqlMapperService.selectOne(sql);
		// 02 ：所选的业务库为注册的贴源库
		if (map != null && map.get("SOURCETYPE") != null && "02".equals(map.get("SOURCETYPE"))) {
			String guid = map.get("GUID").toString(); // 注册的贴源库guid
			sql = "SELECT t.guid,t.tablename,t.tablecname from Raw_Cwtable t WHERE t.centrewarehouseid='" + guid + "' and SOURCETABTYPE='03'";
			List<Map<String, Object>> tableInfo = sqlMapperService.selectList(sql);
			if (tableInfo != null && tableInfo.size() > 0) {
				Map<String, String> mm = new HashMap<String, String>();
				Map<String, String> mm2 = new HashMap<String, String>();
				for (Map<String, Object> m_ : tableInfo) {
					mm.put(m_.get("TABLENAME").toString().toUpperCase(), m_.get("TABLECNAME").toString());
					mm2.put(m_.get("TABLENAME").toString().toUpperCase(), m_.get("GUID").toString());
				}

				/*
				 * if(onlyShowReg != null){ for(Map<String, Object> dm
				 * :dataList){
				 * if(mm.get(dm.get("TABLE_NAME").toString().toUpperCase()) !=
				 * null){ dm.put("TABLE_CNAME",
				 * mm.get(dm.get("TABLE_NAME").toString().toUpperCase()));
				 * lastData.add(dm); } } }else{
				 */
				for (Map<String, Object> dm : dataList) {
					if (mm.get(dm.get("TABLE_NAME").toString().toUpperCase()) != null) {
						dm.put("TABLE_CNAME", mm.get(dm.get("TABLE_NAME").toString().toUpperCase()));
						dm.put("S_TABLEID", mm2.get(dm.get("TABLE_NAME").toString()));
					}
				}
				// return dataList;
				// }
			}
		}
		// return lastData;
	}

	/**
	 * 查看目标库表结构信息
	 * 
	 * @param dataPipelineId
	 *            管道
	 * @param DBtabName
	 *            表名称
	 * @param _type
	 *            建表方式 02 ：从平台 03：从业务库
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/factorsView")
	public ResultBody factorsView(String dataPipelineId, String DBtabName, String _type, String search_str) {
		try {
			// 数据管道
			Map<String, Object> pipeline = this.getPipeline(dataPipelineId);
			if (pipeline == null) {
				ResultBody.createErrorResult("数据管道信息获取失败，请检查！");
			}

			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
			boolean testConn = dynamicConnServiceV2.testConn(dbinfo);
			if (!testConn) {
				return ResultBody.createErrorResult("当前数据管道连接失败，请检查！");
			}

			if (_type != null && "02".equals(_type)) {
				List<Map<String, Object>> factorsList = this.getCWFactorsList_2_0(dbinfo, DBtabName, null);
				return ResultBody.createSuccessResult(factorsList);
			} else {
				// 获取仓库中表的列信息
				List<Map<String, Object>> factorsList = this.getCWFactorsList(dbinfo, DBtabName, null);
				// 加查询条件过滤
				List<Map<String, Object>> result_list = new ArrayList<Map<String, Object>>();
				if (search_str != null && !"".equals(search_str)) {
					for (Map<String, Object> map : factorsList) {
						if (map.get("COLUMN_NAME").toString().toUpperCase().contains(search_str.toUpperCase()) || map.get("COLUMN_CNAME").toString().toUpperCase().contains(search_str.toUpperCase())) {
							result_list.add(map);
						}
					}
				} else {
					result_list.addAll(factorsList);
				}
				return ResultBody.createSuccessResult(result_list);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("码表列信息获取失败！原因：" + e.getMessage());
		}
	}

	/**
	 * 获取表列信息（oracle数据库） 平台表的列信息
	 * 
	 * @param dbinfo
	 * @param DBtabName
	 * @return
	 */
	private List<Map<String, Object>> getCWFactorsList_2_0(DBInfoBean dbinfo, String DBtabName, Object _columns) {
		String filterColumns = "";
		if (_columns != null && !_columns.toString().isEmpty()) {
			Map<String, String> columns = JSONObject.fromObject(_columns);
			filterColumns = "and T.DBCOLUMNCODE in('" + StringUtils.join(columns.keySet().toArray(), "','") + "')";
		}
		String sql = "SELECT T.DBCOLUMNCODE AS COLUMN_NAME,NAME COLUMN_CNAME,T.NULLABLE,"
				+ "decode(T.DATATYPE,'N',(decode(T.SCALE,null,'整型',0,'整型','浮点型')),'字符型') as DATA_TYPE,DATALENGTH as DATA_LENGTH,SCALE as DATA_SCALE " + "FROM FASP_T_DICCOLUMN T WHERE T.TABLECODE = '"
				+ DBtabName.toUpperCase() + "'" + filterColumns + " order by DBCOLUMNCODE";
		return dynamicConnServiceV2.selectList(dbinfo, sql);
	}

	/**
	 * 获取表列信息（常规oracle数据库）
	 * 
	 * @param dbinfo
	 * @param DBtabName
	 * @param _columns
	 * @return
	 */
	private List<Map<String, Object>> getCWFactorsList(DBInfoBean dbinfo, String DBtabName, Object _columns) {

		String filterColumns = "";
		if (_columns != null && !_columns.toString().isEmpty()) {
			Map<String, String> columns = JSONObject.fromObject(_columns);
			filterColumns = "and COLUMN_NAME in('" + StringUtils.join(columns.keySet().toArray(), "','") + "')";
		}
		String sql = "";
		List<Map<String, Object>> FactorsList = new ArrayList<>();
		// 获取仓库中表的列信息
		if ("ORACLE".equals(dbinfo.DBTYPE)) {
			sql = " SELECT COLUMN_NAME,COLUMN_NAME as COLUMN_CNAME,DECODE(NULLABLE,'Y','1','N','0') AS NULLABLE ,DATA_TYPE as DATA_TYPE_,"
					+ " DECODE(DATA_TYPE,'NUMBER',(decode(DATA_SCALE,null,'整型',0,'整型','浮点型')),'DATE','日期型','BLOB','大文本','CLOB','大文本','字符型') as DATA_TYPE,DATA_PRECISION,decode(DATA_TYPE,'CLOB',NULL,'BLOB',NULL,'DATE',NULL,DECODE(DATA_LENGTH,0,1,DATA_LENGTH)) DATA_LENGTH,DATA_SCALE,"
					+ "case when column_name in (" + "select column_name from user_cons_columns where table_name = '" + DBtabName + "' and constraint_name in"
					+ "(select constraint_name from user_constraints where table_name = '" + DBtabName + "' and constraint_type = 'P')) " + "then '1' else '0' end as ISPK "
					+ " from USER_TAB_COLUMNS WHERE table_name = '" + DBtabName.toUpperCase() + "' " + filterColumns + " order by COLUMN_NAME";
			FactorsList = dynamicConnServiceV2.selectList(dbinfo, sql);
		} else if ("MYSQL".equals(dbinfo.DBTYPE)) {
			sql = "select TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,CASE WHEN IS_NULLABLE='NO' THEN '0' ELSE '1' END AS NULLABLE," + "CASE WHEN (DATA_TYPE='int' or DATA_TYPE='bigint') then '整型' "
					+ "WHEN (DATA_TYPE='varchar' or DATA_TYPE='char') THEN '字符型' " + "WHEN DATA_TYPE='double' THEN '浮点型' " + "WHEN DATA_TYPE='float' THEN '浮点型' " + "WHEN DATA_TYPE='date' THEN '日期型' "
					+ "WHEN DATA_TYPE='TIMESTAMP' THEN '日期型' ELSE '字符型' end as DATA_TYPE ," + " DATA_TYPE as DATA_TYPE_,NUMERIC_PRECISION,NUMERIC_SCALE,"
					+ "case when COLUMN_KEY='PRI' THEN '1' ELSE '0' END AS PRIMARY_KEY," + "CHARACTER_MAXIMUM_LENGTH,COLUMN_TYPE " + "from INFORMATION_SCHEMA.columns where TABLE_SCHEMA=\""
					+ dbinfo.DBNAME + "\" and table_name=\"" + DBtabName + "\"";
			FactorsList = dynamicConnServiceV2.selectList(dbinfo, sql);
		} else if (DBTypeConstant.MPPDB.equals(dbinfo.DBTYPE)) {
			sql = "DESCRIBE " + DBtabName;
			List<Map<String, Object>> mppList = new ArrayList<>();
			List<Map<String, Object>> tableList = dynamicConnServiceV2.selectList(dbinfo, sql);
			for (Map<String, Object> map : tableList) {
				Map<String, Object> colMap = new HashMap<>();
				colMap.put("COLUMN_NAME", map.get("name"));
				colMap.put("COLUMN_CNAME", map.get("name"));
				colMap.put("NULLABLE", "true".equals(map.get("nullable")) ? 1 : 0);
				if ("int".equals(map.get("type")) || "bigint".equals(map.get("type"))) {
					colMap.put("DATA_TYPE", "整型");
					colMap.put("DATA_TYPE_", "INT");
				} else if ("timestamp".equals(map.get("type"))) {
					colMap.put("DATA_TYPE", "日期型");
					colMap.put("DATA_TYPE_", "TIMESTAMP");
				} else if ("double".equals(map.get("type"))) {
					colMap.put("DATA_TYPE_", "DOUBLE");
					colMap.put("DATA_TYPE", "浮点型");
				} else {
					colMap.put("DATA_TYPE_", "STRING");
					colMap.put("DATA_TYPE", "字符型");
				}
				colMap.put("PRIMARY_KEY", "true".equals(map.get("primary_key")) ? 1 : 0);
				mppList.add(colMap);
			}
			FactorsList = mppList;
		}else if(DBTypeConstant.GPDB.equals(dbinfo.DBTYPE)){
			sql = "select t.column_name,DATA_TYPE DATA_TYPE_,decode(t.data_type,'bigint','整型','character varying','字符型','timestamp without time zone','日期型','text','大文本','numeric','浮点型') data_type," +
				  "coalesce(t.character_maximum_length,null) data_length,coalesce(t.numeric_precision,null) data_precision,coalesce(t.numeric_scale,null) data_scale," +
				  "decode(t.is_nullable,'YES','1','NO','0') nullable," +
				  "case when column_name in (SELECT a.attname FROM pg_attribute a LEFT JOIN pg_index p ON p.indrelid = a.attrelid AND a.attnum = ANY(p.indkey) WHERE a.attnum > 0 AND NOT a.attisdropped AND  p.indisprimary is not null and p.indisprimary='t' and a.attrelid = '"+DBtabName+"' ::regclass ORDER BY a.attnum) then '1' else '0' end ispk" +
				  " from information_schema.columns t where table_catalog='"+dbinfo.DBNAME+"' and  TABLE_NAME='"+DBtabName.toLowerCase()+"' and table_schema = 'public' " + filterColumns.toLowerCase() ;
			List<Map<String, Object>> tableList = dynamicConnServiceV2.selectList(dbinfo, sql);
			for (Map<String, Object> map : tableList) {
				Map<String, Object> colMap = new HashMap<>();
				colMap.put("COLUMN_NAME", map.get("column_name").toString().toUpperCase());
				colMap.put("COLUMN_CNAME", map.get("column_name").toString().toUpperCase());
				colMap.put("DATA_TYPE", map.get("data_type"));
				colMap.put("DATA_TYPE_", map.get("data_type_"));
				colMap.put("NULLABLE",map.get("nullable"));
				colMap.put("DATA_LENGTH",map.get("data_length"));
				colMap.put("DATA_PRECISION",map.get("data_precision"));
				colMap.put("DATA_SCALE",map.get("data_scale"));
				colMap.put("ISPK",map.get("ispk"));
				FactorsList.add(colMap);
			}

		}
		return FactorsList;
	}

	/**
	 * 获取表列信息（贴源库中的表字段定义）
	 * 
	 * @param DBtabName
	 * @return
	 */
	private List<Map<String, Object>> getLocalFactorsList(String tableId) {
		// 获取仓库中表的列信息
		String sql = "SELECT * from RAW_cwtfactor WHERE tableid='" + tableId + "' order by ORDERNUM,columnname";
		return sqlMapperService.selectList(sql);
	}

	/**
	 * 浏览表数据（默认查看100条数据）：查来源库中的数据
	 * 
	 * @param dataPipelineId
	 *            管道
	 * @param tableCode
	 *            表名称/视图名称 (平台中查表字段用)
	 * @param dbTabName
	 * @param _type
	 *            选择的建表方式 02：从平台 03：从业务库
	 * @param count
	 *            浏览数据行数
	 * @return
	 */
	@RequestMapping("/tableDataView")
	public @ResponseBody
	ResultBody tableDataView(String dataPipelineId, String _type, String tableCode, String dbTabName, int count) {
		try {
			// 数据管道
			Map<String, Object> pipeline = this.getPipeline(dataPipelineId);
			if (pipeline == null) {
				return ResultBody.createErrorResult("数据管道信息获取失败，请检查！");
			}

			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
			boolean testConn = dynamicConnServiceV2.testConn(dbinfo);
			if (!testConn) {
				return ResultBody.createErrorResult("当前数据管道连接失败，请检查！");
			}

			// 获取仓库中表的列信息
			List<Map<String, Object>> factorsList = this.getCWFactorsList(dbinfo, tableCode, null);
			if (factorsList.size() == 0) {
				return ResultBody.createErrorResult("数据检索错误！");
			}
			if ("02".equals(_type)) {
				String tableName = dynamicConnServiceV2.selectOne(dbinfo, "SELECT table_name from user_tables WHERE table_name='" + tableCode.toUpperCase() + "'", String.class);
				if (tableName == null) {
					tableCode = "P#" + tableCode;
				}
			}

			String cols_ = "";
			for (Map<String, Object> cols : factorsList) {
				String DATA_TYPE = cols.get("DATA_TYPE_").toString();
				String COLUMN_NAME = cols.get("COLUMN_NAME").toString();
				if(DBTypeConstant.GPDB.equals(dbinfo.DBTYPE)){
					if(DATA_TYPE.indexOf("timestamp") > -1)
						cols_ += "to_char(" + COLUMN_NAME + ",'yyyy-mm-dd hh:mm:ss') as " + COLUMN_NAME + ",";
					else {
						cols_ += COLUMN_NAME + ",";
					}
				}else {
					if (DATA_TYPE.indexOf("TIMESTAMP") > -1 ) {
						cols_ += "(" + COLUMN_NAME + " + 0) as " + COLUMN_NAME + ",";
					}else {
						cols_ += COLUMN_NAME + ",";
					}
				}
			}

			cols_ = cols_.substring(0, cols_.length() - 1);

			String sql = "select " + cols_ + " from " + tableCode + " where rownum <= " + count;
			if(DBTypeConstant.GPDB.equals(dbinfo.DBTYPE) || DBTypeConstant.MYSQL.equals(dbinfo.DBTYPE))
				sql = "select " + cols_ + " from " + tableCode + " limit " + count;

			// 获取仓库中表数据
			List<Map<String, Object>> dataList = dynamicConnServiceV2.selectList(dbinfo, sql);

			Map<String, Object> resMap = new HashMap<String, Object>();
			resMap.put("factorsList", factorsList);
			resMap.put("dataList", dataList);
			resMap.put("dbType",dbinfo.DBTYPE);
			return ResultBody.createSuccessResult(resMap);
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("系统错误！原因：" + e.getMessage());
		}
	}

	// 数据采集方式
	@RequestMapping("/getCollectType")
	@ResponseBody
	public Map<String, Object> getCollectType() {
		Map<String, Object> map = new HashMap<String, Object>();
		String sql = "SELECT * from bi_code WHERE basename='COLLECTTYPE'";
		List<Map<String, Object>> list = sqlMapperService.selectList(sql);
		for (Map<String, Object> map2 : list) {
			map.put(map2.get("CODEN").toString(), map2.get("CNAME"));
		}
		return map;
	}

	/**
	 * 贴源库浏览表数据，查字段信息
	 * 
	 * @param dbInfoId
	 *            目标管道id
	 * @param tableId
	 *            贴源库中表id
	 * @return
	 */
	@RequestMapping("/tableDataView_cols")
	@ResponseBody
	public ResultBody tableDataView_cols(String dbInfoId, String tableId) {
		try {
			String sql = "select * from bi_t_dbinfo where GUID = '" + dbInfoId + "'";
			// 数据管道
			Map<String, Object> pipeline = sqlMapperService.selectOne(sql);
			if (pipeline == null) {
				return ResultBody.createErrorResult("数据管道信息获取失败，请检查！");
			}

			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
			boolean testConn = dynamicConnServiceV2.testConn(dbinfo);
			if (!testConn) {
				return ResultBody.createErrorResult("当前数据管道连接失败，请检查！");
			}

			// 展示列使用本地贴源库中的字段
			List<Map<String, Object>> factorsList = this.getLocalFactorsList(tableId);
			return ResultBody.createSuccessResult(factorsList);
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("系统错误！原因：" + e.getMessage());
		}
	}

	/**
	 * 浏览表数据（默认查看100条数据）：查目标库中的数据
	 * 
	 * @param dbInfoId
	 *            目标管道id
	 * @param tableId
	 *            贴源库中表id
	 * @param dbTabName
	 *            表名
	 * @return
	 */
	@RequestMapping("/tableDataView_")
	@ResponseBody
	public PageBean<Map<String, Object>> tableDataView_(String dbInfoId, String tableId, String dbTabName, String sourceType, String search_str, String page, String rows, String sidx, String sord) {
		try {
			int start = 0, end = 0;
			if (page != null && rows != null) {
				int p = Integer.parseInt(page);
				int rs = Integer.parseInt(rows);
				start = (p - 1) * rs + 1;
				end = p * rs;
			}
			// 数据管道
			Map<String, Object> pipeline = sqlMapperService.selectOne("select * from bi_t_dbinfo where GUID = '" + dbInfoId + "'");
			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());

			// 获取仓库中表的列信息（此处查询仓库中字段，为处理TIMESTAMP类型字段）
			List<Map<String, Object>> factorsList = this.getCWFactorsList(dbinfo, dbTabName, null);
			String cols_ = "";
			for (Map<String, Object> cols : factorsList) {
				// String DATA_TYPE = cols.get("DATA_TYPE_").toString();
				String COLUMN_NAME = cols.get("COLUMN_NAME").toString();
				if ("日期型".equals(cols.get("DATA_TYPE"))) {
					if (DBTypeConstant.MPPDB.equals(dbinfo.DBTYPE) || DBTypeConstant.MYSQL.equals(dbinfo.DBTYPE)) {
						cols_ += "from_unixtime(unix_timestamp(" + COLUMN_NAME + "),'yyyy-MM-dd HH:mm:ss') as " + COLUMN_NAME + ",";
					}/*else if(DBTypeConstant.MYSQL.equals(dbinfo.DBTYPE))
						cols_ += "TO_CHAR(" + COLUMN_NAME + ",'yyyy-MM-dd HH24:mi:ss') as " + COLUMN_NAME + ",";*/
					else
						cols_ += "TO_CHAR(" + COLUMN_NAME + ",'yyyy-MM-dd HH24:mi:ss') as " + COLUMN_NAME + ",";
				} else {
					cols_ += COLUMN_NAME + ",";
				}
			}
			if(!"".equals(cols_.trim()))
				cols_ = cols_.substring(0, cols_.length() - 1);

			PageBean<Map<String, Object>> pageBean = new PageBean<Map<String, Object>>();
			String sql = "";
			String order = "";
			String tableType = "0";
			tableType = sqlMapperService.selectOne("select is_view from RAW_CWTABLE where guid = '"+tableId+"'",String.class);
			if (sidx != null && !"".equals(sidx)) {
				order = " order by " + sidx + " " + sord;
			}
			if (DBTypeConstant.ORACLE.equals(dbinfo.DBTYPE) || DBTypeConstant.GPDB.equals(dbinfo.DBTYPE)) {
				if("1".equals(tableType))
					sql = "select " + cols_ + " from " + dbTabName ;
				else
					sql = "select " + cols_ + " from " + dbTabName + order;
				if(DBTypeConstant.ORACLE.equals(dbinfo.DBTYPE))
					sql = "select * from (select rownum r,t.* from (" + sql + ") t " + " where rownum <=" + end + " ) where r >=" + start;
				else if(DBTypeConstant.GPDB.equals(dbinfo.DBTYPE))
					sql = "select * from (select row_number() over() r,t.* from (" + sql + ") t  ) b where r <=" + end + " and r >=" + start;
			} else {
				sql = "select " + cols_ + " from " + dbTabName + order + " limit " + end + " offset " + (start - 1);
			}
			// 获取仓库中表数据
			pageBean.setRows(dynamicConnServiceV2.selectList(dbinfo, sql));
			Integer count = dynamicConnServiceV2.selectOne(dbinfo, "SELECT count(1) from " + dbTabName, Integer.class);
			count = (count == null) ? 0 : count;
			pageBean.setTotal(count);
			BigDecimal bi1 = new BigDecimal(count.toString());
			BigDecimal bi2 = new BigDecimal(rows);
			BigDecimal bi3 = bi1.divide(bi2, 0, RoundingMode.UP);
			double totalPage = bi3.doubleValue();
			pageBean.setTotalPage((int) Math.ceil(totalPage));
			return pageBean;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 获取仓库类型
	 * 
	 * @param dbInfoId
	 *            目标管道id
	 * @return
	 */
	@RequestMapping("/getTableType")
	@ResponseBody
	public DBInfoBean getTableType(String dbInfoId) {
		// 数据管道
		Map<String, Object> pipeline = sqlMapperService.selectOne("select * from bi_t_dbinfo where GUID = '" + dbInfoId + "'");
		DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
		return dbinfo;
	}

	/**
	 * 由管道信息构建DBInfoBean
	 * 
	 * @param dbinfoStr
	 * @param DBTYPE
	 * @return
	 */
	private DBInfoBean getDBInfoBeanByPipeline(String DBTYPE, String dbinfoStr) {
		// 数据管道
		JSONObject DBINFO = JSONObject.fromObject(dbinfoStr);
		DBInfoBean dbinfo = new DBInfoBean();
		dbinfo.DBTYPE = DBTYPE;
		dbinfo.HOSTNAME = DBINFO.get("hostName").toString();
		dbinfo.PORTNUMBER = DBINFO.get("portNumber").toString();
		if ("ORACLE".equals(dbinfo.DBTYPE)) {
			dbinfo.SERVICENAME = DBINFO.get("serviceName").toString();
		} else {
			dbinfo.DBNAME = DBINFO.get("DBName").toString();
		}
		if (!"HBASE".equals(dbinfo.DBTYPE) && !DBTypeConstant.MPPDB.equals(dbinfo.DBTYPE)) {
			dbinfo.DBUID = DBINFO.get("DBUid").toString();
			dbinfo.DBPWD = DBINFO.get("DBPwd").toString();
		}

		return dbinfo;
	}

	// 数据管道详情 BI_DATAPIPELINE
	private Map<String, Object> getPipeline(String dataPipelineId) {
		return sqlMapperService.selectOne("select * from BI_DATAPIPELINE where GUID='" + dataPipelineId + "' ");
	}

	/**
	 * 贴源库表是否存在
	 * 
	 * @param centreWarehouseId
	 *            库id
	 * @param tableName
	 *            物理表名
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/checkTabIsExist")
	public ResultBody checkTabIsExist(String centreWarehouseId, String tableName) {
		String sql = "SELECT TABLENAME FROM RAW_CWTABLE WHERE centreWarehouseId='" + centreWarehouseId + "' and UPPER(tablename) = '" + tableName.toUpperCase() + "'";
		String tabName = sqlMapperService.selectOne(sql, String.class);
		return ResultBody.createSuccessResult(tabName == null ? false : true);

	}
	/**
	 * 贴源库表是否存在
	 *
	 * @param centreWarehouseId
	 *            库id
	 * @param tableName
	 *            物理表名
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/checkTabCNameIsExist")
	public ResultBody checkTabCNameIsExist(String centreWarehouseId, String tableCName) {
		String sql = "SELECT TABLECNAME FROM RAW_CWTABLE WHERE centreWarehouseId='" + centreWarehouseId + "' and TABLECNAME = '" + tableCName + "'";
		String tabName = sqlMapperService.selectOne(sql, String.class);
		return ResultBody.createSuccessResult(tabName == null ? false : true);

	}

	/**
	 * 贴源库表是否存在（批量建表中，校验多个表）
	 * 
	 * @param request
	 * @param centreWarehouseId
	 *            库id
	 * @param rows
	 *            当前需要创建的表的行对象
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/checkTabsIsExist")
	public ResultBody checkTabsIsExist(HttpServletRequest request, String centreWarehouseId, String rows) {
		try {
			List<Map<String, Object>> list = JSONArray.fromObject(rows);
			String tables = "";
			for (Map<String, Object> map : list) {
				tables += "'" + map.get("tableName").toString().toUpperCase() + "',";
			}
			tables = tables.substring(0, tables.length() - 1);

			String sql = "SELECT TABLENAME FROM RAW_CWTABLE WHERE centreWarehouseId='" + centreWarehouseId + "' and UPPER(tablename) IN(" + tables + ")";
			List<Map<String, Object>> dataList = sqlMapperService.selectList(sql);
			return ResultBody.createSuccessResult(dataList);
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("系统出错了！信息:" + e.getMessage());
		}
	}

	/**
	 * 贴源库批量建表
	 * 
	 * @param centreWarehouseId
	 *            库id
	 * @param rows
	 *            需要创建的表和列信息
	 * @param _type
	 *            "04"-根据资源目录批量创建； "05"-主数据
	 * @param isRunTrans
	 *            "0" -不执行抽取 "1" -执行抽取
	 * @return
	 * @author HRK
	 * @date 2019年3月28日
	 */
	@SuppressWarnings("unchecked")
	@ResponseBody
	@RequestMapping("/executeCreateTableByResource")
	public ResultBody executeCreateTableByResource(HttpServletRequest request, String centreWarehouseId, String params, String _type, String versionId, String catId, String isRunTrans) {
		try {
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
			// 校验目标管道连接
			Map<String, Object> targetPipe = this.getTargetPipe(centreWarehouseId);
			if (targetPipe == null) {
				ResultBody.createErrorResult("目标数据管道信息获取失败，请检查！");
			}

			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());
			dbinfo.DBNAMECN = targetPipe.get("PIPELINENAME_REAL").toString();
			boolean testConn = dynamicConnServiceV2.testConn(dbinfo);
			if (!testConn) {
				return ResultBody.createErrorResult("目标数据管道连接失败，请检查！");
			}

			// 前台表信息
			Map<String, Object> row_table = JSONObject.fromObject(params);

			// 获取源表的列信息
			List<Map<String, Object>> factors = this.getColumns(_type, row_table);

			Map<String, Object> columns = JSONObject.fromObject(row_table.get("_columns"));

			List<Map<String, Object>> temp = new ArrayList<Map<String, Object>>();

			factors = this.getCWFactors(columns, factors);
			temp.addAll(factors);
			// 创建仓库物理表
			ResultBody res_create = this.createCDWTable(dbinfo, row_table.get("tableName").toString(), row_table.get("tableCName").toString(), factors, null);
			if (res_create.isError) {
				return res_create;
			}
			// 保存贴源库表定义
			boolean saveCWTableDefin = this.saveCWTableDefin(centreWarehouseId, row_table, temp, _type, targetPipe.get("GUID").toString(), user.UserName, catId, user.MID, "");
			if (!saveCWTableDefin) {
				// 贴源库定义保存失败，删除仓库表
				this.dropCDWTable(dbinfo, row_table.get("tableName").toString());
				return ResultBody.createErrorResult("贴源库定义保存失败，请检查贴源库表定义！");
			}

			String tableId = sqlMapperService.selectOne("select t.GUID from RAW_CWTABLE t where t.TABLENAME='" + row_table.get("tableName").toString() + "' and t.CENTREWAREHOUSEID='"
					+ centreWarehouseId + "'", String.class);
			// 检查来源和目标管道连接
			DBInfoBean inDBInfoBean = mdmService.getDBInfoBean_MDM();
			// 如果来自主数据,保存血缘关系数据并创建转换
			if ("05".equals(_type)) {
				// 创建转换
				ResultBody res_creatTrans = this.createTabTrans_MDMtoRAW(centreWarehouseId, inDBInfoBean, dbinfo, row_table.get("TABLE_NAME").toString(), row_table.get("TABLE_CNAME").toString(),
						factors, user.optAdmdivCode, row_table.get("tableName").toString());
				if (res_creatTrans.isError) {
					this.dropCDWTable(dbinfo, row_table.get("tableName").toString());// 删除仓库表
					this.delCDWTabFactor(tableId);// 删除逻辑表
					return res_creatTrans;
				}
				Map<String, Object> _map = (Map<String, Object>) res_creatTrans.result;
				String transId = _map.get("transId").toString();// 转换id

				// 保存转换信息
				mfbicommonService.create_eltransinfo(user.optAdmdivCode, Integer.parseInt(transId), centreWarehouseId, tableId, "01", "1", user.UserName, "0");
				// 保存血缘关系
				mfbicommonService.saveDLS_RelRaw_MdmToRaw(centreWarehouseId, _type, inDBInfoBean, row_table);
				if ("1".equals(isRunTrans)) {// 是否执行转换
					ResultBody runTrans = packageService.runTrans2(user.optAdmdivCode, Integer.parseInt(transId));
					if (runTrans.isError) {
						return ResultBody.createErrorResult("执行转换失败!<br>" + runTrans.errMsg);
					}
				}

			} else {// 来自资源目录
				Map<String, Object> resMap = new HashMap<>();
				resMap.put("msg", res_create.result);
				resMap.put("tableid", tableId);
				return ResultBody.createSuccessResult(resMap);
			}

			return res_create;
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("创建失败！信息:" + e.getMessage());
		}
	}

	/**
	 * 从主数据到贴源库创建转换
	 * 
	 * @param centreWarehouseId
	 *            贴源库id
	 * @param dbinfo_s
	 *            主数据DB
	 * @param dbinfo_t
	 *            目标DB
	 * @param TABLE_NAME
	 *            源表名
	 * @param transName
	 *            转换名
	 * @param factors
	 *            源表的列信息
	 * @param admdivCode
	 * @param tableName
	 *            目标表表名
	 * @return
	 * @throws Exception
	 */
	private ResultBody createTabTrans_MDMtoRAW(String centreWarehouseId, DBInfoBean dbinfo_s, DBInfoBean dbinfo_t, String TABLE_NAME, String transName, List<Map<String, Object>> factors,
			String admdivCode, String tableName) throws Exception {
		String sql = "SELECT t.cname rawNAME,t2.cname CATENAME from Raw_Centrewarehouse t, raw_datacategory t2" + " WHERE t.datacategoryid = t2.id AND T.GUID='" + centreWarehouseId + "'";
		Map<String, Object> rawObj = sqlMapperService.selectOne(sql);

		StringBuffer sbf = new StringBuffer();
		List<TableInputOutField> fields = new ArrayList<TableInputOutField>();
		for (Map<String, Object> m : factors) {
			String columnName = m.get("COLUMNNAME").toString();
			String factorType = m.get("DATATYPE").toString();
			String _type = TableInputOutField.TYPE_STRING;
			if ("整型".equals(factorType) || "浮点型".equals(factorType)) {
				_type = TableInputOutField.TYPE_NUMBER;
			} else if ("日期型".equals(factorType)) {
				_type = TableInputOutField.TYPE_DATE;
			}
			TableInputOutField tableInputOutField = new TableInputOutField(columnName, columnName, _type);
			fields.add(tableInputOutField);
			sbf.append(columnName + ",");
		}
		// 表输入SQL
		String selectSql = "select " + sbf.substring(0, sbf.length() - 1) + " from " + TABLE_NAME + " t ";

		JSONObject params = new JSONObject();
		params.put("dataSource", EtlConstant.SOURCE_ZSJ);
		params.put("description", "从主数据批量创建贴源库表");
		TransMetaConfig transMetaConfig = rawService.getTransMetaConfig("0", dbinfo_s, dbinfo_t, selectSql, tableName, fields, transName, MfbiCommon.SYSDIRECTORY + "/" + MfbiCommon.SOURCE_TYK + "/"
				+ rawObj.get("CATENAME") + "/" + rawObj.get("RAWNAME"), params);
		ResultBody result_createTrans = packageService.createTrans(admdivCode, transMetaConfig);
		if (result_createTrans.isError) {
			return result_createTrans;
		}
		Map<String, Object> _map = (Map<String, Object>) result_createTrans.result;
		String transId = _map.get("transId").toString();// 转换id

		// 记录转换ID到表定义上
		String sql_ = "update RAW_CWTABLE set TRANSID=" + _map.get("transId") + " where CENTREWAREHOUSEID='" + centreWarehouseId + "' and upper(TABLENAME)='" + tableName.toUpperCase() + "'";
		sqlMapperService.execSql2(sql_);

		return result_createTrans;
	}

	// 取列信息（主数据或资源目录）
	private List<Map<String, Object>> getColumns(String _type, Map<String, Object> row_table) {
		String sql = "";
		if ("04".equals(_type)) {
			sql = "select t.ITEMNAME as COLUMN_NAME, t.ITEMCNAME as COLUMN_CNAME,'1' as NULLABLE"
					+ ", (case when tt.ENTITY ='整型' then '整型' when tt.ENTITY = '日期型' then '日期型' when tt.ENTITY = '浮点型' then '浮点型' WHEN tt.entity='大文本型C' THEN '大文本' else '字符型' end) as DATA_TYPE"
					+ ", t.ITEMWIDTH as DATA_LENGTH, t.DECWIDTH as DATA_SCALE,t.SORTID as ORDERNUM" + " from drc_t_resourceitem t " + " left join drc_t_basecode tt on t.datatype = tt.code"
					+ " where t.sublistid='" + row_table.get("tableId").toString() + "'" + " and t.itemname in (" + row_table.get("fNameStr").toString() + ")";
		} else if ("05".equals(_type)) {
			sql = "select t.RELACOLUMNNAME as COLUMN_NAME, t.FACTORNAME as COLUMN_CNAME,nvl(T.ISEMPTY,'1') as NULLABLE"
					+ ", (case tt.NAME when '日期型' then '日期型' when '整型' then '整型' when '浮点型' then '浮点型' else '字符型' end) as DATA_TYPE"
					+ ", t.DEFVALUE as DATA_DEFAULT, t.FACTORWIDTH as DATA_LENGTH, t.DECWIDTH as DATA_SCALE,t.SORTID as ORDERNUM" + " from mdm_t_factor t"
					+ " left join MDM_basecode tt on t.factortype = tt.code " + " where modelid='" + row_table.get("tableId").toString() + "'" + " and versionno = '"
					+ row_table.get("versionId").toString() + "'" + " and (t.isvisible = '1' or relacolumnname in(" + row_table.get("fNameStr").toString() + "))" + " order by t.sortid,t.isbase desc";
		}
		// 获取表的列信息
		List<Map<String, Object>> factorsList = mdmService.getList(sql);
		return factorsList;
	}

	/**
	 * 创建表定义(此方法只处理sourceTabType=02、03)
	 * 
	 * @param request
	 * @param centreWarehouseId
	 *            库id
	 * @param dataPipelineId
	 *            来源管道id
	 * @param rows
	 *            当前需要创建的表的行数据
	 * @param sourceTabType
	 *            来源表类型 01：常规手工创建 02：来源平台 03：来源业务库 04：来源资源目录 05：来源主数据
	 * @param catId
	 *            贴源库分类id
	 * @param isRunTrans
	 *            是否执行抽取 1：是 0：否
	 * @return
	 */
	@SuppressWarnings("unchecked")
	@ResponseBody
	@RequestMapping("/executeCreateTable")
	public ResultBody executeCreateTable(HttpServletRequest request, String centreWarehouseId, String dataPipelineId, String row, String sourceTabType, String catId, String isRunTrans) {
		try {
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
			// 管道
			Map<String, Object> pipeline = this.getPipeline(dataPipelineId);
			if (pipeline == null) {
				return ResultBody.createErrorResult("业务库数据管道信息获取失败，请检查！");
			}
			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
			boolean testConn = dynamicConnServiceV2.testConn(dbinfo);
			if (!testConn) {
				return ResultBody.createErrorResult("业务库数据管道连接失败，请检查管道信息是否正确！");
			}

			Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId); // 仓库目标管道
			if (targetPipe == null) {
				ResultBody.createErrorResult("目标数据管道信息获取失败，请检查！");
			}

			DBInfoBean dbinfo_t = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());
			boolean testConn_t = dynamicConnServiceV2.testConn(dbinfo_t);
			if (!testConn_t) {
				return ResultBody.createErrorResult("目标数据管道连接失败，请检查！");
			}

			// 前台表信息
			Map<String, Object> row_table = JSONObject.fromObject(row);

			// 获取源表的列信息
			List<Map<String, Object>> factors = null;
			if ("02".equals(sourceTabType)) {
				factors = getCWFactorsList_2_0(dbinfo, row_table.get("TABLE_NAME").toString(), row_table.get("_columns"));
			} else {// 其他业务库
				if (row_table.get("S_TABLEID") != null) { // 该业务表来源注册的贴源库
					factors = this.getCWFactorsList_reg(row_table, dataPipelineId, row_table.get("_columns"));
				} else {
					factors = this.getCWFactorsList(dbinfo, row_table.get("TABLE_NAME").toString(), row_table.get("_columns"));
				}
			}
			Map<String, Object> columns = JSONObject.fromObject(row_table.get("_columns"));
			factors = this.getCWFactors(columns, factors);

			List<Map<String, Object>> temp = new ArrayList<Map<String, Object>>();
			temp.addAll(factors);

			// 创建仓库物理表
			ResultBody createCDWTable = this.createCDWTable(dbinfo_t, row_table.get("tableName").toString(), row_table.get("tableCName").toString(), factors, null);
			if (createCDWTable.isError) {
				return createCDWTable;
			}

			// 保存贴源库表定义
			boolean saveCWTableDefin = this.saveCWTableDefin(centreWarehouseId, row_table, temp, sourceTabType, targetPipe.get("GUID").toString(), user.UserName, catId, user.MID, dataPipelineId);
			if (!saveCWTableDefin) {
				// 贴源库定义保存失败，删除仓库表
				this.dropCDWTable(dbinfo_t, row_table.get("tableName").toString());
				return ResultBody.createErrorResult("贴源库定义保存失败，请检查贴源库表定义！");
			}

			// 保存来源表信息、字段信息等，用于重建转换
			// rawService.saveSourceTableInfo(centreWarehouseId,dataPipelineId,row_table,factors);

			dbinfo.DBNAMECN = pipeline.get("PIPELINENAME").toString();
			dbinfo_t.DBNAMECN = targetPipe.get("PIPELINENAME_REAL").toString();

			String tableId = sqlMapperService.selectOne("select t.GUID from RAW_CWTABLE t where t.TABLENAME='" + row_table.get("tableName").toString() + "' and t.CENTREWAREHOUSEID='"
					+ centreWarehouseId + "'", String.class);
			// 创建转换
			ResultBody transRes = this.createRawTableTrans(centreWarehouseId, dbinfo, dbinfo_t, row_table.get("TABLE_NAME").toString(), row_table.get("tableCName").toString(), factors,
					user.optAdmdivCode, row_table.get("tableName").toString());
			if (transRes.isError) {
				// 删除物理表
				this.dropCDWTable(dbinfo_t, row_table.get("tableName").toString());
				// 删除表中字段定义
				this.delCDWTabFactor(tableId);
				return transRes;
			}
			Map<String, Object> _map = (Map<String, Object>) transRes.result;
			String transId = _map.get("transId").toString();// 转换id
			// 保存转换信息
			mfbicommonService.create_eltransinfo(user.optAdmdivCode, Integer.parseInt(transId), centreWarehouseId, tableId, "01", "1", user.UserName, "0");
			// 保存血缘关系数据
			mfbicommonService.saveDLS_RelRaw_RawToRaw(dataPipelineId, centreWarehouseId, row_table, dbinfo, dbinfo_t);
			// 记录转换ID到来源信息主表上
			// String sql2_ =
			// "update raw_source_infomain set TRANSID = "+transId +
			// " where guid='"
			// +centreWarehouseId+"' and upper(TABLENAME)='"+row_table.get("tableName").toString().toUpperCase()+"'";
			// sqlMapperService.execSql2(sql2_);
			if ("1".equals(isRunTrans)) { // 是否执行转换
				ResultBody runTrans = packageService.runTrans2(user.optAdmdivCode, Integer.parseInt(transId));
				if (runTrans.isError) {
					return ResultBody.createErrorResult("执行转换失败!<br>" + runTrans.errMsg);
				}
			}

			// 为新建的表默认选择采集方式(数据库直采)
			setDefaultCollection(centreWarehouseId, dataPipelineId, tableId);

			return transRes;
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("创建失败！信息:" + e.getMessage());
		} finally {
		}
	}

	/**
	 * 删除逻辑定义表和字段表
	 * 
	 * @param tableid
	 */
	private void delCDWTabFactor(String tableid) {
		List<String> sqls = new ArrayList<String>();
		// 删除表中字段定义
		sqls.add("DELETE RAW_cwtfactor WHERE tableid='" + tableid + "'");
		// 删除表定义
		sqls.add("DELETE RAW_CWTABLE WHERE guid='" + tableid + "'");
		sqlMapperService.execSqls(sqls);
	}

	/**
	 * 
	 * @param centreWarehouseId
	 *            贴源库id
	 * @param dataPipelineId
	 *            来源管道id
	 * @param tableId
	 *            表id
	 */
	public void setDefaultCollection(String centreWarehouseId, String dataPipelineId, String tableId) {
		// 查看当前的来源管道是否已经配置连接
		Boolean isCreate = dataPipelinet_reuse(centreWarehouseId, dataPipelineId, null);
		if (isCreate) {
			// 已配置连接 ,直接为表选择此连接
			this.selectCollect(centreWarehouseId, dataPipelineId, tableId);
		} else {
			// 未配置连接,新建连接,并为当前表选择
			String dbconnName = mfbicommonService.getPipeline(dataPipelineId).get("PIPELINENAME").toString();
			String sql_creatConn = "insert into raw_etl_dbconnection (rawid,datapipelineId,connectionName) values('" + centreWarehouseId + "','" + dataPipelineId + "','" + dbconnName + "')";
			sqlMapperService.insert(sql_creatConn);
			this.selectCollect(centreWarehouseId, dataPipelineId, tableId);
		}
	}

	// 为表选择数据库直采的连接
	private void selectCollect(String centreWarehouseId, String dataPipelineId, String tableId) {
		String connId = sqlMapperService.selectOne("select GUID from raw_etl_dbconnection where rawid='" + centreWarehouseId + "' and datapipelineid='" + dataPipelineId + "'", String.class);
		String sql_collect = "insert into raw_etl_tableconfig(RAWID,TABLEID,COLLECTTYPE,CONNECTIONID)values('" + centreWarehouseId + "','" + tableId + "','01','"
				+ (("".equals(connId) || null == connId) ? "" : connId) + "')";
		sqlMapperService.insert(sql_collect);
	}

	/**
	 * 从业务库批量创建贴源库表-- 创建转换
	 * 
	 * @param centreWarehouseId
	 *            贴源库id
	 * @param dbinfo
	 *            表输入连接信息
	 * @param dbinfo_t
	 *            表输出连接信息
	 * @param TABLE_NAME
	 *            表输入
	 * @param transName
	 *            转换名称
	 * @param factors
	 *            表输入字段信息
	 * @param admdivCode
	 *            区划
	 * @param tableName
	 *            贴源库目标表
	 * @return
	 * @throws Exception
	 */
	private ResultBody createRawTableTrans(String centreWarehouseId, DBInfoBean dbinfo, DBInfoBean dbinfo_t, String TABLE_NAME, String transName, List<Map<String, Object>> factors, String admdivCode,
			String tableName) {

		//String sql = "SELECT t.cname rawNAME,t2.cname CATENAME from Raw_Centrewarehouse t, raw_datacategory t2" + " WHERE t.datacategoryid = t2.id AND T.GUID='" + centreWarehouseId + "'";
		String sql = "SELECT t.cname rawNAME, (select LISTAGG(cname,'/')  WITHIN GROUP(ORDER BY PID)  from raw_datacategory A  start with id = (select B.datacategoryid \n" +
				"                    from Raw_Centrewarehouse B\n" +
				"                   where B.guid = '"+centreWarehouseId+"')\n" +
				"CONNECT BY PRIOR A.pid = A.id ) CATENAME\n" +
				"  from Raw_Centrewarehouse t, raw_datacategory t2\n" +
				" WHERE t.datacategoryid = t2.id\n" +
				"   AND T.GUID = '"+centreWarehouseId+"'";
		Map<String, Object> rawObj = sqlMapperService.selectOne(sql);

		StringBuffer sbf = new StringBuffer();
		List<TableInputOutField> fields = new ArrayList<TableInputOutField>();
		for (Map<String, Object> m : factors) {
			String columnName = m.get("COLUMNNAME").toString();
			String factorType = m.get("DATATYPE").toString();
			String _type = TableInputOutField.TYPE_STRING;
			if ("整型".equals(factorType) || "浮点型".equals(factorType)) {
				_type = TableInputOutField.TYPE_NUMBER;
			} else if ("日期型".equals(factorType)) {
				_type = TableInputOutField.TYPE_DATE;
			}
			TableInputOutField tableInputOutField = new TableInputOutField(columnName, columnName, _type);
			fields.add(tableInputOutField);
			sbf.append(columnName + ",");
		}
		// 表输入SQL
		String selectSql = "select " + sbf.substring(0, sbf.length() - 1) + " from " + TABLE_NAME + " t ";

		JSONObject params = new JSONObject();
		params.put("dataSource", EtlConstant.SOURCE_TYK);
		params.put("description", "从业务库批量创建贴源库表");
		TransMetaConfig transMetaConfig = rawService.getTransMetaConfig("0", dbinfo, dbinfo_t, selectSql, tableName, fields, transName, MfbiCommon.SYSDIRECTORY + "/" + MfbiCommon.SOURCE_TYK + "/"
				+ rawObj.get("CATENAME") + "/" + rawObj.get("RAWNAME"), params);
		ResultBody result_createTrans = packageService.createTrans(admdivCode, transMetaConfig);
		if (result_createTrans.isError) {
			return result_createTrans;
		}
		Map<String, Object> _map = (Map<String, Object>) result_createTrans.result;
		String transId = _map.get("transId").toString();// 转换id

		// 记录转换ID到表定义上
		String sql_ = "update RAW_CWTABLE set TRANSID=" + _map.get("transId") + " where CENTREWAREHOUSEID='" + centreWarehouseId + "' and upper(TABLENAME)='" + tableName.toUpperCase() + "'";
		sqlMapperService.execSql2(sql_);

		/*
		 * ResultBody runTrans = packageService.runTrans2(admdivCode,
		 * Integer.parseInt(transId)); if(runTrans.isError) return
		 * ResultBody.createErrorResult("执行转换失败!<br>"+runTrans.errMsg);
		 */
		return result_createTrans;
	}

	private List<Map<String, Object>> getCWFactorsList_reg(Map<String, Object> row_table, String dataPipelineId, Object _columns) {
		String filterColumns = "";
		if (_columns != null && !_columns.toString().isEmpty()) {
			Map<String, String> columns = JSONObject.fromObject(_columns);
			filterColumns = "and COLUMNNAME in('" + StringUtils.join(columns.keySet().toArray(), "','") + "')";
		}
		String sql = "SELECT r.sourcetype,r.guid from raw_centrewarehouse r,bi_t_dbinfo t WHERE r.guid=t.centrewarehouseid AND t.pipelineid='" + dataPipelineId + "'";
		Map<String, Object> map = sqlMapperService.selectOne(sql);
		// 02 ：所选的业务库为注册的贴源库
		if (map != null && map.get("SOURCETYPE") != null && "02".equals(map.get("SOURCETYPE"))) {
			sql = "SELECT COLUMNNAME COLUMN_NAME,COLUMNCNAME as COLUMN_CNAME,NOTNULL NULLABLE,"
					+ "DATATYPE DATA_TYPE_,DATATYPE DATA_TYPE,DATA_LENGTH,DATA_SCALE,ISPK FROM RAW_cwtfactor WHERE TABLEID='" + row_table.get("S_TABLEID") + "' " + filterColumns;
			return sqlMapperService.selectList(sql);
		}
		return null;
	}

	private void dropCDWTable(DBInfoBean dbinfo, String dbTableName) {
		dynamicConnServiceV2.execSql(dbinfo, "drop table " + dbTableName);
		dynamicConnServiceV2.execSql(dbinfo, "drop table " + dbTableName + "_b");
	}

	/**
	 * 保存贴源库表定义 -- zmx
	 * 
	 * @param centreWarehouseId
	 *            仓库id
	 * @param table
	 *            表定义信息
	 * @param factors
	 *            字段定义
	 * @param sourceTabType
	 *            资源来源类型
	 * @param targetPipeId
	 *            目标管道（仓库）id
	 * @param userName
	 *            用户
	 * @param sourceDataPipelineId
	 *            来源管道id（业务库管道ID）
	 * @return
	 */
	private boolean saveCWTableDefin(String centreWarehouseId, Map<String, Object> table, List<Map<String, Object>> factors, String sourceTabType, String targetPipeId, String userName, String catId,
			String userId, String sourceDataPipelineId) {
		List<String> sqls = new ArrayList<String>();
		String sql = "select sys_guid() from dual";
		String guid = sqlMapperService.selectOne(sql, String.class);
		// 贴源库表定义
		sqls.add("INSERT INTO RAW_CWTABLE (guid,tablename, tablecname, centrewarehouseid,"
				+ "createdate,SOURCETABNAME,SOURCETABCNAME,SOURCETABTYPE,DBINFOID,creater,DRC_VERSIONID,CATID,CREATEUSER,CREATEUSERID,SOURCEPIPELINEID) " + "VALUES('"
				+ guid
				+ "', '"
				+ table.get("tableName")
				+ "', '"
				+ table.get("tableCName")
				+ "','"
				+ centreWarehouseId
				+ "',sysdate,'"
				+ table.get("TABLE_NAME")
				+ "','"
				+ table.get("TABLE_CNAME")
				+ "','"
				+ sourceTabType
				+ "','"
				+ targetPipeId
				+ "','"
				+ userName
				+ "','"
				+ (("04".equals(sourceTabType) || "05".equals(sourceTabType)) ? table.get("versionId") : "")
				+ "','"
				+ catId
				+ "','" + userName + "','" + userId + "','" + sourceDataPipelineId + "')");

		// 贴源库字段定义
		for (Map<String, Object> facts : factors) {
			Integer len = null;
			if (facts.get("DATA_LENGTH") != JSONNull.getInstance() && facts.get("DATA_LENGTH") != null && !"".equals(facts.get("DATA_LENGTH"))) {
				len = Integer.valueOf(facts.get("DATA_LENGTH").toString());
			}
			Integer scale = null;
			if (facts.get("DATA_SCALE") != JSONNull.getInstance() && facts.get("DATA_SCALE") != null && !"".equals(facts.get("DATA_SCALE"))) {
				scale = Integer.valueOf(facts.get("DATA_SCALE").toString());
			}
			sqls.add("INSERT INTO RAW_cwtfactor (COLUMNNAME,COLUMNCNAME,DATATYPE,DATA_LENGTH,DATA_SCALE, NOTNULL, TABLEID,ISPK,ISPARTITION,PARTITION_ORDER,ORDERNUM,CREATEUSER,CREATEUSERID) "
					+ "VALUES('"
					+ facts.get("COLUMN_NAME")
					+ "','"
					+ facts.get("COLUMN_CNAME")
					+ "','"
					+ facts.get("DATA_TYPE")
					+ "',"
					+ len
					+ ","
					+ scale
					+ ", '"
					+ facts.get("NOTNULL")
					+ "', '"
					+ guid
					+ "','"
					+ facts.get("ISPK")
					+ "','"
					+ (facts.get("ISPARTITION") == JSONNull.getInstance() || facts.get("ISPARTITION") == null ? "0" : facts.get("ISPARTITION"))
					+ "','"
					+ (facts.get("PARTITION_ORDER") == JSONNull.getInstance() || facts.get("PARTITION_ORDER") == null ? "" : facts.get("PARTITION_ORDER"))
					+ "','"
					+ (facts.get("ORDERNUM") == JSONNull.getInstance() || facts.get("ORDERNUM") == null ? "" : facts.get("ORDERNUM")) + "','" + userName + "','" + userId + "')");
		}
		try {
			Integer exec = sqlMapperService.execSqls(sqls);
			if (exec != null && exec == -1) {
				return false;
			}
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * 设置主键，分区字段
	 * 
	 * @param columns
	 * @param factors
	 * @return
	 */
	private List<Map<String, Object>> getCWFactors(Map<String, Object> columns, List<Map<String, Object>> factors) {

		for (Map<String, Object> facts : factors) {
			// 来源库中表结构
			// COLUMN_NAME,COLUMN_CNAME,NULLABLE
			// ,DATA_TYPE_,DATA_TYPE,DATA_PRECISION,DATA_LENGTH,DATA_SCALE
			// 贴源库表结构
			// COLUMNNAME,COLUMNCNAME,DATATYPE,DATA_LENGTH,DATA_SCALE, NOTNULL,
			// TABLEID,ISPK,ISPARTITION,PARTITION_ORDER
			String COLUMN_NAME = facts.get("COLUMN_NAME").toString();
			facts.put("COLUMNNAME", COLUMN_NAME);
			facts.put("COLUMNCNAME", facts.get("COLUMN_CNAME"));
			facts.put("DATATYPE", facts.get("DATA_TYPE"));
			facts.put("NOTNULL", facts.get("NULLABLE"));
			if (facts.get("DATA_DEFAULT") != null && facts.get("DATA_DEFAULT").toString().startsWith("$") && facts.get("DATA_DEFAULT").toString().endsWith("$")) {
				facts.put("DATA_DEFAULT", "");
			}
			String dataType = facts.get("DATA_TYPE").toString();
			if ("浮点型".equals(dataType) && facts.get("DATA_PRECISION") != null) {
				facts.put("DATA_LENGTH", Integer.valueOf(facts.get("DATA_PRECISION").toString()));
			}
			if (columns != null && columns.get(COLUMN_NAME) != null) {
				// columns_ 值存储顺序为： COLUMN_NAME,ISPK,ISPARTITION,PARTITION_ORDER
				String[] columns_ = columns.get(COLUMN_NAME).toString().split(",");
				facts.put("ISPK", columns_[1]);// 主键
				if ("1".equals(columns_[1])) {
					facts.put("NOTNULL", "0"); // 是主键，则不允许为空
				}
				facts.put("ISPARTITION", columns_[2]); // 是否为分区字段
				if (columns_.length > 3) {
					facts.put("PARTITION_ORDER", columns_[3]); // 分区字段顺序号
				}
				if (columns_.length > 4 && (columns_[4] != null && !"".equals(columns_[4]))) {
					facts.put("ORDERNUM", columns_[4]); // 顺序号
				}
				if (columns_.length > 5 && (columns_[5] != null && !"".equals(columns_[5]))) {
					facts.put("COLUMN_CNAME", columns_[5]); // 中文名
				}
			}
		}
		return factors;
	}

	private Map<String, List<Map<String, Object>>> getTableMultiSourceinfo(List<Map<String, Object>> busiTableList, List<Map<String, Object>> busiTableColsList,
			List<Map<String, Object>> seletedywkTableList, List<Map<String, Object>> tableLinksList, StringBuffer sbf_centent) {

		return null;
	}

	/**
	 * 创建仓库物理表 -- zmx
	 * 
	 * @param dbinfo
	 * @param row_table
	 *            表定义信息
	 * @param factors
	 *            字段信息
	 * @return
	 */
	private ResultBody createCDWTable(DBInfoBean dbinfo, String tableName, String tableCName, List<Map<String, Object>> factors, String editKey) {
		try {
			Integer generateCWTable = dwService.generateCWTable(dbinfo, factors, tableName, editKey, false);
			if (generateCWTable == -1) {
				return ResultBody.createErrorResult("创建失败！");
			} else {
				return ResultBody.createSuccessResult("成功创建表：</br>" + tableCName + "【" + tableName + "】");
			}
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("创建失败！信息:" + e.getMessage());
		}
	}

	/***************************** 表end ********************************************/

	/***************************** 生成物理库部分 ********************************************/

	/**
	 * 从贴源库创建事实、创建维度、创建资源目录、创建主数据 获取库中的表
	 * 
	 * @param centreWarehouseId
	 *            库id
	 * @return
	 */
	@ResponseBody
	@RequestMapping(value = "/getTableList", method = RequestMethod.POST)
	public PageBean<Map<String, Object>> getTableList(String centreWarehouseId, String sbtid, String page, String rows, String search_str) {
		try {
			int start = 0, end = 0;
			if (page != null && rows != null) {
				int p = Integer.parseInt(page);
				int rs = Integer.parseInt(rows);
				start = (p - 1) * rs + 1;
				end = p * rs;
			}
			// 查询条件
			String sql = "SELECT * from RAW_CWTABLE d WHERE d.centrewarehouseid = '" + centreWarehouseId + "'";
			if (search_str != null && !search_str.isEmpty()) {
				search_str = search_str.toUpperCase();
				sql += " and (UPPER(d.tablename) like '%" + search_str + "%' or UPPER(d.tablecname) like '%" + search_str + "%') ";
			}
			if (sbtid != null && !"".equals(sbtid)) {
				sql += " and d.tablename not in(select b.SOURCETABLENAME from bi_cw_table b where b.cwid='" + sbtid + "' and b.sourcetype ='2' and b.sourcetablename is not null)";
			}

			String sqlPage = "select * from (select rownum r,t.* from (" + sql + " order by tablename) t " + " where rownum <=" + end + " ) where r >=" + start;

			List<Map<String, Object>> dataList = sqlMapperService.selectList(sqlPage);
			Integer count = sqlMapperService.selectOne("select count(t.tablename) from (" + sql + ") t", Integer.class);
			PageBean<Map<String, Object>> pageBean = new PageBean<Map<String, Object>>();
			pageBean.setRows(dataList);
			pageBean.setTotal(count == null ? 0 : count);
			return pageBean;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private ResultBody generateCWTable_(DBInfoBean dbinfo, Map<String, Object> rowMap) {
		try {
			// zmxs
			// dwService.generateCWTable(dbinfo, rowMap.get("GUID").toString(),
			// rowMap.get("TABLENAME").toString());///构建表
			return ResultBody.createSuccessResult(true);
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("系统出错了！信息:" + e.getMessage());
		}
	}

	/**
	 * 记录构建库操作日志（已作废）
	 * 
	 * @param session
	 * @param centrewarehouseid
	 *            库id
	 * @param dataPipelineId
	 *            管道id
	 * @param dbInfo
	 *            数据库连接信息（DBTYPE，HOSTNAME，PORTNUMBER，SERVICENAME，DBUID，DBPWD，
	 *            DBNAME）
	 */
	/*
	 * @ResponseBody
	 * 
	 * @RequestMapping("/insertBuildLog") public void cwBuildLog(HttpSession
	 * session,String centrewarehouseid,String dataPipelineId,String
	 * dbInfo,String dbInfo_hive) { try { UserBean user = (UserBean)
	 * session.getAttribute("CurUser"); JSONObject jObj =
	 * JSONObject.fromObject(dbInfo); List<String> sqls = new
	 * ArrayList<String>(); String sql =
	 * "insert into bi_cwbuild_log(GUID, centrewarehouseid, datapipelineid, dbinfo, dbtype,DBUID,DBPWD, OPERATOR, build_time) values("
	 * +
	 * "sys_guid(),'"+centrewarehouseid+"','"+dataPipelineId+"','"+dbInfo+"','"
	 * +jObj
	 * .get("DBTYPE")+"','"+jObj.get("DBUid")+"','"+jObj.get("DBPwd")+"','"+
	 * user.LogID+"',to_char(sysdate,'yyyy/mm/dd hh24:mi:ss'))"; sqls.add(sql);
	 * //当构建的管道为hive时，则记录hive的元数据管道信息 if("HIVE".equals(jObj.get("DBTYPE"))){
	 * JSONObject jObj_h = JSONObject.fromObject(dbInfo_hive); sql =
	 * "select centrewarehouseid from bi_subject_hive where centrewarehouseid='"
	 * +centrewarehouseid+"'"; String id = sqlMapperService.selectOne(sql,
	 * String.class); if(id != null){ sql =
	 * "UPDATE bi_subject_hive SET dbname = '"
	 * +jObj_h.get("DBNAME")+"', dbtype = '"
	 * +jObj_h.get("DBType")+"',  dbuid = '"
	 * +jObj_h.get("DBUid")+"',dbpwd = '"+jObj_h
	 * .get("DBPwd")+"',hostname = '"+jObj_h
	 * .get("hostName")+"', portnumber = '"+
	 * jObj_h.get("portNumber")+"', servicename = '"
	 * +jObj_h.get("serviceName")+"',socketid = '"
	 * +dataPipelineId+"',dwsourcetype = '2' where centrewarehouseid = '"
	 * +centrewarehouseid+"'"; }else{ sql =
	 * "INSERT INTO bi_subject_hive(dbname, dbtype, dbuid, dbpwd, hostname, portnumber, servicename, socketid, dwsourcetype, centrewarehouseid) VALUES("
	 * +
	 * "'"+jObj.get("DBNAME")+"', '"+jObj_h.get("DBType")+"', '"+jObj_h.get("DBUid"
	 * )
	 * +"', '"+jObj_h.get("DBPwd")+"', '"+jObj_h.get("hostName")+"','"+jObj_h.get
	 * ("portNumber")+"', '"+jObj_h.get("serviceName")+"', '"+jObj_h.get(
	 * "dataPipelineId")+"', '2', '"+centrewarehouseid+"')"; } sqls.add(sql); }
	 * sqlMapperService.execSqls(sqls); } catch (Exception e) {
	 * e.printStackTrace(); } }
	 */

	/**
	 * 取贴源库构建物理库日志记录（已作废）
	 * 
	 * @param session
	 * @param centrewarehouseid
	 */
	/*
	 * @ResponseBody
	 * 
	 * @RequestMapping("/getBuildLog") public List<Map<String,Object>>
	 * getBuildLog(HttpSession session,String centrewarehouseid) { try {
	 * UserBean user = (UserBean) session.getAttribute("CurUser"); String sql =
	 * "select * from (select * from bi_cwbuild_log where OPERATOR='"
	 * +user.LogID+"' and CENTREWAREHOUSEID='"+centrewarehouseid+
	 * "' order by build_time desc) where ROWNUM = 1 "; return
	 * sqlMapperService.selectList(sql); } catch (Exception e) {
	 * e.printStackTrace(); return null; } }
	 */

	@ResponseBody
	@RequestMapping("/getSubjectHiveDBInfo")
	public List<Map<String, Object>> getSubjectHiveDBInfo(String centrewarehouseid) {
		try {
			String sql = "select * from bi_subject_hive where CENTREWAREHOUSEID='" + centrewarehouseid + "'";
			return sqlMapperService.selectList(sql);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/************************************* 从已有的仓库反向生成贴源库定义 *************************************************/

	/**
	 * 注册贴源库，查询已有物理库中的表
	 * 
	 * @param dataPipelineId
	 *            管道id
	 * @param page
	 *            当前第几页 ，由easyui grid 自动传递
	 * @param rows
	 *            每页查询行数 ，由easyui grid 自动传递
	 * @param search_str
	 *            搜索条件
	 * @param filterTables
	 *            过滤已存在的表
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/queryCWTableList")
	public PageBean<Map<String, Object>> queryCWTableList(String dataPipelineId, String page, String rows, String search_str, String filterTables,String ohterFilter) {
		// 查询条件
		Map<String, Object> pipeline = this.getPipeline(dataPipelineId);
		DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
		return this.getCWTables(dbinfo, page, rows, search_str, filterTables,ohterFilter);
	}

	/**
	 * 注册的贴源库，注册表，过滤已注册过的物理表
	 * @param centreWarehouseId
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getExistCDWTables")
	public List<Map<String, Object>> getExistCDWTables(String centreWarehouseId,String filter) {
		try {
			if(filter ==null)
				filter = "";
			return sqlMapperService.selectList("SELECT TABLENAME from RAW_CWTABLE where centreWarehouseId='" + centreWarehouseId + "' "+filter+" order by tablename");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	// 过滤已注册的表 not in 打散处理，防止not in 项超出
	private String filterTablesCondi(String filterTables,String dbType){
		StringBuffer sbf = new StringBuffer();
		String[] arr = filterTables.split(",");
		int len = arr.length;
		int range = 500; // 最大500个，大于500 分成多组
		if(len > range){
			for(int i =0;i< Math.round((len / range));i++){
				List<String> list = Arrays.asList(Arrays.copyOfRange(arr, (range * i), (range * i + range)));
				String str = StringUtils.join(list.toArray(), ",");
				if (DBTypeConstant.MPPDB.equals(dbType))
					sbf.append(" AND TBL_NAME NOT IN(" + str + ")");
				else
					sbf.append(" AND table_name NOT IN(" + str + ")");
			}
			return sbf.toString();
		}
		return DBTypeConstant.MPPDB.equals(dbType) ? " AND TBL_NAME NOT IN(" + filterTables + ")" : " AND table_name NOT IN(" + filterTables + ")";
	}

	private PageBean<Map<String, Object>> getCWTables(DBInfoBean dbinfo, String page, String rows, String search_str, String filterTables,String ohterFilter) {
		int start = 0, end = 0;
		int num = 0;
		if (page != null && rows != null) {
			int p = Integer.parseInt(page);
			int rs = Integer.parseInt(rows);
			start = (p - 1) * rs + 1;
			end = p * rs;
		}
		if (Integer.parseInt(page) == 1) {
			num = end;
		}
		PageBean<Map<String, Object>> pageBean = new PageBean<Map<String, Object>>();
		String where = " where 1 = 1";
		if (search_str != null && !search_str.trim().isEmpty()) {
			where += " and table_NAME like '%" + search_str.trim().toUpperCase() + "%'";
		}
		if (filterTables != null && !filterTables.trim().isEmpty()) {
			where += this.filterTablesCondi(filterTables,dbinfo.DBTYPE);
		}
		if(ohterFilter != null){
			where += " "+ohterFilter;
		}
		Integer count = null;
		if ("ORACLE".equals(dbinfo.DBTYPE)) {
			String sql = "SELECT table_name tableName,table_type from user_tab_comments " + where + " ORDER BY table_type ,table_NAME ";
			sql = "select * from (select rownum r,t.* from (" + sql + ") t " + " where rownum <=" + end + " ) where r >=" + start;

			pageBean.setRows(dynamicConnServiceV2.selectList(dbinfo, sql));
			count = dynamicConnServiceV2.selectOne(dbinfo, "SELECT count(table_name)  from user_tab_comments " + where, Integer.class);
		} else if ("MYSQL".equals(dbinfo.DBTYPE)) {
			where += " and table_schema = '" + dbinfo.DBNAME + "'";
			String sql = "select table_name TABLENAME,TABLE_TYPE from information_schema.tables " + where + " ORDER BY table_type ,table_NAME limit " + num + " offset " + (end - num);
			pageBean.setRows(dynamicConnServiceV2.selectList(dbinfo, sql));
			count = dynamicConnServiceV2.selectOne(dbinfo, "select count(table_name) from information_schema.tables " + where, Integer.class);
		} else if (DBTypeConstant.MPPDB.equals(dbinfo.DBTYPE)) {
			String DBName = dbinfo.DBNAME;
			String sql = "select TBL_NAME TABLENAME,TBL_TYPE TABLE_TYPE from TBLS t, DBS d " + where + " and t.db_id = d.db_id and d.name = '" + DBName + "' order by TBL_TYPE,TBL_NAME limit " + num
					+ " offset " + (end - num);
			DBInfoBean dbInfo_hive = commonService.getServerProperties("impala_hive_oracle.properties");
			dbInfo_hive.DBPWD = AesUtil.aesEncrypt(dbInfo_hive.DBPWD);
			try {
				dynamicConnServiceV2.testConn(dbInfo_hive);
				pageBean.setRows(dynamicConnServiceV2.selectList(dbInfo_hive, sql));
				count = dynamicConnServiceV2.selectOne(dbInfo_hive, "select count(TBL_NAME) from TBLS t, DBS d " + where + " and t.db_id = d.db_id and d.name = '" + DBName + "'", Integer.class);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}else if(DBTypeConstant.GPDB.equals(dbinfo.DBTYPE)){
			String sql = "SELECT table_name TABLENAME,table_type TABLE_TYPE from information_schema.tables "+where+" and table_catalog='"+dbinfo.DBNAME+"' and table_schema='public' order by table_name,table_type ";
			sql = "select * from (select row_number() over() r,t.* from (" + sql + ") t ) a where r <= "+end+" and r >=" + start;
			pageBean.setRows(dynamicConnServiceV2.selectList(dbinfo, sql));
			count = dynamicConnServiceV2.selectOne(dbinfo, "SELECT count(table_name)  from information_schema.tables " + where +" and table_catalog='"+dbinfo.DBNAME+"' and table_schema='public'", Integer.class);
		}
		pageBean.setTotal(count == null ? 0 : count);
		return pageBean;
	}

	/**
	 * 业务库注册
	 * @param categoryId 仓库分类id
	 * @param cName 仓库名称
	 * @param remark 备注信息
	 * @param dataPipelineId 管道id
	 * @param rows 要生成的表和列信息
	 * @param  RESTOREDWGUID 还原库管道GUID
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/otherCWcreateTableDefin")
	public ResultBody otherCWcreateTableDefin(HttpServletRequest request, String categoryId, String cName,
					String remark, String dataPipelineId, String rows, String sourceType,String RESTOREDWGUID,String RESTOREDW_TEXT) {
		try {
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
			String sql_guid = "select sys_guid() guid from dual";
			String centrewarehouseId = sqlMapperService.selectOne(sql_guid, String.class);
			// 管道
			Map<String, Object> pipeline = this.getPipeline(dataPipelineId);
			if (pipeline == null) {
				return ResultBody.createErrorResult("数据管道信息获取失败，请检查  ！");
			}
			String pipeName = pipeline.get("PIPELINENAME").toString();
			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());

			boolean testConn = dynamicConnServiceV2.testConn(dbinfo);
			if (!testConn) {
				return ResultBody.createErrorResult("数据管道连接失败，请检查管道信息是否正确！");
			}
			ResultBody emptyJob = null;
			String rawDirectories = null;
			String jobId = "";
			// 还原库管道GUID
			if(RESTOREDWGUID != null){
				// 注册贴源库，预置一个空作业
				rawDirectories = this.getRawDirectories(categoryId);
				emptyJob = jobService.createEmptyJob(user.optAdmdivCode, cName, rawDirectories + "/" + cName);
				if(emptyJob.isError)
					return emptyJob;
				jobId = ((Map<String,Object>)emptyJob.result).get("jobId").toString();
				// 记录作业id到mfbi转换信息中间表，再次注册表需要将新注册的表对应的转换加入到该作业中
				mfbicommonService.create_eltransinfo(user.optAdmdivCode,Integer.valueOf(jobId), centrewarehouseId, centrewarehouseId, "00", "2", user.UserName, "0");
			}

			List<String> sqls = new ArrayList<String>();
			// 新增库信息定义
			sqls.add("insert into RAW_centrewarehouse(guid,cname,REMARK,DATACATEGORYID,CREATER,sourceType,PROVINCE,CREATERID,RESTOREDWGUID) values('" + centrewarehouseId + "','" + cName + "','" + remark + "','"
					+ categoryId + "','" + user.UserName + "','" + sourceType + "','" + user.optAdmdivCode + "','" + user.MID + "','"+(RESTOREDWGUID != null ? RESTOREDWGUID:"")+"')");
			// 贴源库表分类 -默认分类
			sqls.add("insert into raw_cwtablecat(GUID,NAME,CENTREWAREHOUSEID,ORDERNUM) values('default','默认分类','" + centrewarehouseId + "',0)");
			String dbInfoId = sqlMapperService.selectOne(sql_guid, String.class);
			// 目标库目标管道信息
			String sql_pipel1 = "insert into BI_T_DBINFO (guid,CENTREWAREHOUSEID, PIPELINEID, PIPELINENAME, DBTYPE, DBINFO,CREATER,CREATERID) " + " values('" + dbInfoId + "','" + centrewarehouseId
					+ "', '" + dataPipelineId + "','" + pipeline.get("PIPELINENAME") + "','" + dbinfo.DBTYPE + "','" + pipeline.get("DBINFO") + "','" + user.UserName + "','" + user.MID + "')";
			sqls.add(sql_pipel1);

			List<String> sqls_CW = new ArrayList<String>();

			List<Map<String, Object>> rows_ = JSONArray.fromObject(rows);
			for (Map<String, Object> map : rows_) {
				String tableId = sqlMapperService.selectOne(sql_guid, String.class);
				String tableName = map.get("TABLENAME").toString();
				String tableCName = map.get("TABLECNAME").toString();
				if(RESTOREDWGUID != null && !"".equals(RESTOREDWGUID)){
					ResultBody res = tranService.crateTransTYK2(user.optAdmdivCode, RESTOREDW_TEXT, pipeName, tableName, rawDirectories + "/" + cName, cName, jobId);
					if(res.isError)
						return res;
					String transIds = (String) res.result;
					String[] arr = transIds.split(",");
					for(String transid : arr)// 记录注册表的转换信息
						mfbicommonService.create_eltransinfo(user.optAdmdivCode,Integer.valueOf(transid), centrewarehouseId, tableId, "00", "1", user.UserName, "0");
				}

				// 增加表定义
				sqls.add("insert into RAW_CWTABLE(guid,TABLENAME,TABLECNAME,CENTREWAREHOUSEID,createdate,SOURCETABNAME,SOURCETABCNAME,SOURCETABTYPE,DBINFOID,creater) values('" + tableId + "','"
						+ tableName + "','" + tableCName + "','" + centrewarehouseId + "',sysdate,'" + tableName + "','','03','" + dbInfoId + "','" + user.LogID + "')");

				Map<String, String> columns = new HashMap<String, String>();
				if (map.get("_columns") != null && !map.get("_columns").toString().isEmpty()) {
					columns = JSONObject.fromObject(map.get("_columns"));
				}

				boolean isExistField_OPER_TIME = false;
				boolean isExistField_DWTAB_EXPIRATION_TIME = false;
				List<Map<String, Object>> cwFactorsList = getCWFactorsList(dbinfo, tableName, map.get("_columns"));
				// 增加字段定义
				for (Map<String, Object> facts : cwFactorsList) {
					String colcName = columns.get(facts.get("COLUMN_NAME"));
					int ordernum = 0;
					if (colcName != null) {
						ordernum = Integer.valueOf(colcName.split("_@_")[1] + "");
						colcName = colcName.split("_@_")[0];
					}

					if (DBTypeConstant.MPPDB.equals(dbinfo.DBTYPE)) {
						sqls.add("INSERT INTO RAW_cwtfactor (COLUMNNAME,COLUMNCNAME,DATATYPE, NOTNULL, TABLEID,ISPK,ORDERNUM) VALUES('" + facts.get("COLUMN_NAME").toString().toUpperCase() + "','"
								+ (colcName != null ? colcName : facts.get("COLUMN_NAME").toString().toUpperCase()) + "','" + facts.get("DATA_TYPE") + "','" + facts.get("NULLABLE") + "','" + tableId
								+ "','" + facts.get("PRIMARY_KEY") + "'," + ordernum + ")");
					} else if ("ORACLE".equals(dbinfo.DBTYPE)) {
						String dataType = facts.get("DATA_TYPE_").toString();
						Integer len = null;
						if (facts.get("DATA_LENGTH") != null && !(facts.get("DATA_LENGTH").toString().isEmpty())) {
							len = Integer.valueOf(facts.get("DATA_LENGTH").toString());
						}
						if ("NUMBER".equals(dataType)) {
							Object scale = facts.get("DATA_SCALE");
							if (scale != null && Integer.valueOf(scale.toString()) > 0) {
								len = Integer.valueOf(facts.get("DATA_PRECISION").toString());
							} else {
								len = null;
							}
						}
						if ("DATE".equals(dataType) || "CLOB".equals(dataType)) {
							len = null;
						}
						sqls.add("INSERT INTO RAW_cwtfactor (COLUMNNAME,COLUMNCNAME,DATATYPE,DATA_LENGTH,DATA_SCALE, NOTNULL, TABLEID,ISPK,ORDERNUM) VALUES('" + facts.get("COLUMN_NAME") + "','"
								+ (colcName != null ? colcName : facts.get("COLUMN_NAME").toString().toUpperCase()) + "','" + facts.get("DATA_TYPE") + "'," + len + ","
								+ (len == null ? null : facts.get("DATA_SCALE")) + ", '" + facts.get("NULLABLE") + "', '" + tableId + "','" + facts.get("ISPK") + "'," + ordernum + ")");
					} else if ("MYSQL".equals(dbinfo.DBTYPE)) {
						String dataType = facts.get("DATA_TYPE_").toString();
						Integer len = null;
						if ("varchar".equals(dataType)) {
							len = Integer.valueOf(facts.get("CHARACTER_MAXIMUM_LENGTH").toString());
						} else if ("TIMESTAMP".equals(dataType) || "date".equals(dataType)) {
							len = null;
						} else if ("int".equals(dataType) || "BLOB".equalsIgnoreCase(dataType)) {
							len = null;
						} else if ("double".equals(dataType) || "float".equals(dataType)) {
							len = Integer.valueOf(facts.get("NUMERIC_PRECISION").toString());
						}
						sqls.add("INSERT INTO RAW_cwtfactor (COLUMNNAME,COLUMNCNAME,DATATYPE,DATA_LENGTH,DATA_SCALE, NOTNULL, TABLEID,ISPK,ORDERNUM) VALUES('"
								+ facts.get("COLUMN_NAME").toString().toUpperCase() + "','" + (colcName != null ? colcName : facts.get("COLUMN_NAME").toString().toUpperCase()) + "','"
								+ facts.get("DATA_TYPE").toString().toUpperCase() + "'," + len + "," + (len == null ? null : facts.get("NUMERIC_SCALE")) + ", '" + facts.get("NULLABLE") + "', '"
								+ tableId + "','" + facts.get("PRIMARY_KEY") + "'," + ordernum + ")");
					}
					// 仓库中是否存在OPER_TIME字段
					if ("DWTAB_CREATE_TIME".equals(facts.get("COLUMN_NAME").toString().toUpperCase())) {
						isExistField_OPER_TIME = true;
					}
					if ("DWTAB_EXPIRATION_TIME".equals(facts.get("COLUMN_NAME").toString().toUpperCase())) {
						isExistField_DWTAB_EXPIRATION_TIME = true;
					}
				}
				if (!isExistField_OPER_TIME) {
					if ("ORACLE".equals(dbinfo.DBTYPE)) {
						sqls_CW.add("ALTER TABLE " + tableName + " ADD DWTAB_CREATE_TIME date DEFAULT sysdate");
					} else if (DBTypeConstant.MPPDB.equals(dbinfo.DBTYPE)) {
						sqls_CW.add("ALTER TABLE " + tableName + " ADD COLUMNS(DWTAB_CREATE_TIME TIMESTAMP DEFAULT now())");
					} else if ("MYSQL".equals(dbinfo.DBTYPE)) {
						sqls_CW.add("alter table " + tableName + " add column DWTAB_CREATE_TIME TIMESTAMP(2)");
					}
					sqls.add("INSERT INTO RAW_cwtfactor (COLUMNNAME,COLUMNCNAME,DATATYPE,DATA_LENGTH,DATA_SCALE, NOTNULL, TABLEID,ISPK) VALUES('DWTAB_CREATE_TIME','DWTAB_CREATE_TIME','日期型'," + null
							+ "," + null + ", '1', '" + tableId + "','0')");
				}
				if (!isExistField_DWTAB_EXPIRATION_TIME) {
					if ("ORACLE".equals(dbinfo.DBTYPE)) {
						sqls_CW.add("ALTER TABLE " + tableName + " ADD DWTAB_EXPIRATION_TIME NUMBER");
					} else if (DBTypeConstant.MPPDB.equals(dbinfo.DBTYPE)) {
						// sqls_CW.add("ALTER TABLE " + tableName +
						// " ADD COLUMNS(OPER_TIME TIMESTAMP DEFAULT sysdate)");
						sqls_CW.add("ALTER TABLE " + tableName + " ADD COLUMNS(DWTAB_EXPIRATION_TIME string)");
					} else if ("MYSQL".equals(dbinfo.DBTYPE)) {
						sqls_CW.add("alter table " + tableName + " add column DWTAB_EXPIRATION_TIME INT");
					}
					sqls.add("INSERT INTO RAW_cwtfactor (COLUMNNAME,COLUMNCNAME,DATATYPE,DATA_LENGTH,DATA_SCALE, NOTNULL, TABLEID,ISPK) VALUES('DWTAB_EXPIRATION_TIME','DWTAB_EXPIRATION_TIME','整型',"
							+ null + "," + null + ", '1', '" + tableId + "','0')");
				}
			}
			sqlMapperService.execSqls(sqls);
			dynamicConnServiceV2.execDdls2(dbinfo, sqls_CW);
			return ResultBody.createSuccessResult(centrewarehouseId);
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult(e.getMessage());
		}
	}

	/**
	 * 注册的贴源库，增加注册表
	 * @param request
	 * @param rows
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/registTableDefin")
	public ResultBody registTableDefin(HttpServletRequest request, String dbInfoId, String rows, String catId) {
		try {
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");

			String centreWarehouseId = request.getParameter("centreWarehouseId");
			String sql = "SELECT t.cname,t.restoredwguid,t1.pipelinename,t.DATACATEGORYID from RAW_CENTREWAREHOUSE t LEFT JOIN bi_datapipeline t1 ON t.restoredwguid = t1.guid where t.guid='"+centreWarehouseId+"'";
			Map<String, Object> rawObj = sqlMapperService.selectOne(sql);
			//String cName = rawObj.get("CNAME").toString();

			// 校验目标管道连接
			Map<String, Object> targetPipe = this.getTargetPipe(centreWarehouseId);
			if (targetPipe == null) {
				return ResultBody.createErrorResult("数据管道信息获取失败，请检查！");
			}

			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());
			boolean testConn = dynamicConnServiceV2.testConn(dbinfo);
			if (!testConn) {
				return ResultBody.createErrorResult("数据管道连接失败，请检查管道信息是否正确！");
			}
			//String rawDirectories = this.getRawDirectories(rawObj.get("DATACATEGORYID").toString());

			List<String> sqls = new ArrayList<String>();
			String sql_guid = "select sys_guid() guid from dual";
			List<Map<String, Object>> rows_ = JSONArray.fromObject(rows);
			List<String> sqls_CW = new ArrayList<String>();
			for (Map<String, Object> map : rows_) {
				String tableId = sqlMapperService.selectOne(sql_guid, String.class);
				String tableName = map.get("TABLENAME").toString();
				String tableCName = map.get("TABLECNAME").toString();
				String type = "0";
				if((DBTypeConstant.GPDB.equals(dbinfo.DBTYPE) || DBTypeConstant.MPPDB.equals(dbinfo.DBTYPE)) && "VIEW".equals(map.get("table_type").toString()) )
					type = "1";
				else if("VIEW".equals(map.get("TABLE_TYPE").toString()))
					type = "1";
				// 将新注册的表加入到自动作业中  RESTOREDWGUID:注册库设置的还原库管道GUID
				/*if(rawObj.get("RESTOREDWGUID") != null){
					String jobId = sqlMapperService.selectOne("SELECT etlobjectid from bi_t_etltransinfo t WHERE t.busiobjid='"+centreWarehouseId+"' and t.busitype='00'",String.class);
					ResultBody res = tranService.crateTransTYK2(user.optAdmdivCode, rawObj.get("PIPELINENAME").toString(), targetPipe.get("PIPELINENAME").toString(), tableName, rawDirectories + "/" + cName, cName, jobId);
					if(res.isError)
						return res;
				}*/
				// 增加表定义
				sqls.add("insert into RAW_CWTABLE(guid,TABLENAME,TABLECNAME,CENTREWAREHOUSEID,createdate,SOURCETABNAME,SOURCETABCNAME,SOURCETABTYPE,DBINFOID,creater,catId,is_view) values('" + tableId + "','"
						+ tableName + "','" + tableCName + "','" + centreWarehouseId + "',sysdate,'" + tableName + "','','03','" + targetPipe.get("GUID") + "','" + user.LogID + "','" + catId + "','"+type+"')");

				List<Map<String, Object>> cwFactorsList = getCWFactorsList(dbinfo, tableName, map.get("_columns"));
				Map<String, String> columns = new HashMap<String, String>();
				if (map.get("_columns") != null && !map.get("_columns").toString().isEmpty()) {
					columns = JSONObject.fromObject(map.get("_columns"));
				}
				// 增加字段定义
				for (int i =0;i<cwFactorsList.size();i++) {
					Map<String, Object> facts = cwFactorsList.get(i);
					String colcName = columns.get(facts.get("COLUMN_NAME"));
					int ordernum = 0;
					if (colcName != null) {
						if(!colcName.split("_@_")[1].equals("null"))
							ordernum = Integer.valueOf(colcName.split("_@_")[1] + "");
						colcName = colcName.split("_@_")[0];
					}else
						ordernum = i +1;
					this.addRAW_cwtfactor(sqls,colcName,dbinfo,facts,ordernum,tableId);
				}
			}
			sqlMapperService.execSqls(sqls);
			dynamicConnServiceV2.execDdls2(dbinfo, sqls_CW);
			return ResultBody.createSuccessResult("操作成功！");
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult(e.getMessage());
		}
	}

	private void addRAW_cwtfactor(List<String> sqls,String colcName,DBInfoBean dbinfo,Map<String,Object> facts,int ordernum,String tableId){
		if (DBTypeConstant.MPPDB.equals(dbinfo.DBTYPE)) {
			sqls.add("INSERT INTO RAW_cwtfactor (COLUMNNAME,COLUMNCNAME,DATATYPE, NOTNULL, TABLEID,ISPK,ORDERNUM) VALUES('" + facts.get("COLUMN_NAME").toString().toUpperCase() + "','"
					+ (colcName != null ? colcName : facts.get("COLUMN_NAME").toString().toUpperCase()) + "','" + facts.get("DATA_TYPE") + "','" + facts.get("NULLABLE") + "','" + tableId
					+ "','" + facts.get("PRIMARY_KEY") + "'," + ordernum + ")");
		} else if ("ORACLE".equals(dbinfo.DBTYPE)) {
			String dataType = facts.get("DATA_TYPE_").toString();
			Integer len = null;
			Object scale = facts.get("DATA_SCALE");
			if ("NUMBER".equals(dataType)) {
				if (scale != null && Integer.valueOf(scale.toString()) > 0) {
					len = Integer.valueOf(facts.get("DATA_PRECISION").toString());
				} else {
					len = 22;
				}
			} else if ("DATE".equals(dataType) || "CLOB".equals(dataType) || "BLOB".equals(dataType)) {
				len = null;
			} else {
				len = Integer.valueOf(facts.get("DATA_LENGTH").toString());
			}
			sqls.add("INSERT INTO RAW_cwtfactor (COLUMNNAME,COLUMNCNAME,DATATYPE,DATA_LENGTH,DATA_SCALE, NOTNULL, TABLEID,ISPK,ORDERNUM) VALUES('" + facts.get("COLUMN_NAME") + "','"
					+ (colcName != null ? colcName : facts.get("COLUMN_CNAME")) + "','" + facts.get("DATA_TYPE") + "'," + len + "," + (len == null ? null : facts.get("DATA_SCALE"))
					+ ", '" + facts.get("NULLABLE") + "', '" + tableId + "','" + facts.get("ISPK") + "'," + ordernum + ")");
		} else if ("MYSQL".equals(dbinfo.DBTYPE)) {
			String dataType = facts.get("DATA_TYPE_").toString();
			Integer len = null;
			if ("varchar".equals(dataType) || "char".equals(dataType)) {
				len = Integer.valueOf(facts.get("CHARACTER_MAXIMUM_LENGTH").toString());
			} else if ("TIMESTAMP".equals(dataType) || "date".equals(dataType)) {
				len = null;
			} else if ("int".equals(dataType)) {
				len = null;
			} else if ("double".equals(dataType) || "float".equals(dataType)) {
				len = Integer.valueOf(facts.get("NUMERIC_PRECISION").toString());
			}
			sqls.add("INSERT INTO RAW_cwtfactor (COLUMNNAME,COLUMNCNAME,DATATYPE,DATA_LENGTH,DATA_SCALE, NOTNULL, TABLEID,ISPK,ORDERNUM) VALUES('"
					+ facts.get("COLUMN_NAME").toString().toUpperCase() + "','" + (colcName != null ? colcName : facts.get("COLUMN_NAME").toString().toUpperCase()) + "','"
					+ facts.get("DATA_TYPE").toString().toUpperCase() + "'," + len + "," + (len == null ? null : facts.get("NUMERIC_SCALE")) + ", '" + facts.get("NULLABLE") + "', '"
					+ tableId + "','" + facts.get("PRIMARY_KEY") + "'," + ordernum + ")");
		}else if(DBTypeConstant.GPDB.equals(dbinfo.DBTYPE)){
			String dataType = facts.get("DATA_TYPE_").toString();
			Integer len = null;
			Object scale = facts.get("DATA_SCALE");
			if ("numeric".equals(dataType))
				len = Integer.valueOf(facts.get("DATA_PRECISION").toString());
			else if ("timestamp without time zone".equals(dataType) || "text".equals(dataType) || "bigint".equals(dataType))
				len = null;
			else
				len = Integer.valueOf(facts.get("DATA_LENGTH").toString());

			sqls.add("INSERT INTO RAW_cwtfactor (COLUMNNAME,COLUMNCNAME,DATATYPE,DATA_LENGTH,DATA_SCALE, NOTNULL, TABLEID,ISPK,ORDERNUM) VALUES('" + facts.get("COLUMN_NAME") + "','"
					+ (colcName != null ? colcName : facts.get("COLUMN_CNAME")) + "','" + facts.get("DATA_TYPE") + "'," + len + "," + (len == null ? null : facts.get("DATA_SCALE"))
					+ ", '" + facts.get("NULLABLE") + "', '" + tableId + "','" + facts.get("ISPK") + "'," + ordernum + ")");
		}
	}

	/**
	 * 获取目标库管道列表
	 * 
	 * @param centreWarehouseId
	 *            贴源库id
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getPipeline_dbinfo")
	public List<Map<String, Object>> getPipeline_dbinfo(String centreWarehouseId) {
		try {
			return sqlMapperService.selectList("SELECT * from bi_t_dbinfo t WHERE t.centrewarehouseid='" + centreWarehouseId + "' ORDER BY t.pipelinename");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	// 复制库
	@SuppressWarnings("unchecked")
	@ResponseBody
	@RequestMapping("/executeCopyCW")
	public ResultBody executeCopyCW(HttpServletRequest request, String centreWarehouseId, String row, String oldWarehouseId, String buildType) {
		try {
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
			Map<String, Object> targetPipe = this.getTargetPipe(centreWarehouseId);
			if (targetPipe == null) {
				return ResultBody.createErrorResult("目标数据管道信息获取失败，请检查！");
			}
			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());
			boolean testConn = dynamicConnServiceV2.testConn(dbinfo);
			if (!testConn) {
				return ResultBody.createErrorResult("目标数据管道连接失败，请检查！");
			}
			dbinfo.DBNAMECN = targetPipe.get("PIPELINENAME").toString();

			Map<String, String> tab_map = JSONObject.fromObject(row);
			// 字段信息
			List<Map<String, Object>> factorList = new ArrayList<>();
			if (!"".equals(tab_map.get("columnNames_").toString().trim())) {

				String[] columnName_ = tab_map.get("columnNames_").toString().split(",");
				Map<String, String> tabCol = JSONObject.fromObject(tab_map.get("columnNames"));
				// List<Map<String,Object>> cols = JSONArray.fromObject(tabCol);
				for (int i = 0; i < columnName_.length; i++) {
					String[] colInfos = tabCol.get(columnName_[i]).toString().split(",");
					Map<String, Object> map = new HashMap<>();
					map.put("COLUMNNAME", colInfos[0]);
					map.put("COLUMNCNAME", colInfos[1]);
					map.put("DATATYPE", colInfos[2]);
					map.put("ISPK", colInfos[3]);
					map.put("NOTNULL", colInfos[4]);
					map.put("DATA_LENGTH", colInfos[5]);
					map.put("DATA_SCALE", colInfos[6]);
					map.put("ISPARTITION", colInfos[7]);
					map.put("PARTITION_ORDER", colInfos[8]);
					map.put("ORDERNUM", colInfos[9]);
					map.put("INPUTRULES ", colInfos[10]);
					factorList.add(map);
				}
			}
			// String columnNames =
			// tab_map.get("columnNames")==null?"":" and COLUMNNAME in(" +
			// tab_map.get("columnNames").toString() + ")";
			// String sql_seFactor =
			// "select * from RAW_cwtfactor where TABLEID='" +
			// tab_map.get("GUID") + "' " + columnNames;
			// List<Map<String, Object>> factorList =
			// sqlMapperService.selectList(sql_seFactor);
			// 创建表定义和物理表
			ResultBody result_tab = executeCopyCW_tab(dbinfo, tab_map, centreWarehouseId, targetPipe.get("GUID").toString(), user.LogID, factorList, user.MID, user.UserName);
			if (result_tab.isError) {
				return result_tab;
			}
			// 是否复制数据
			if ("1".equals(buildType)) {
				// 执行转换
				ResultBody result_trans = executeCopyCW_trans(centreWarehouseId, oldWarehouseId, dbinfo, tab_map, factorList, user.optAdmdivCode);
				if (result_trans.isError) {
					return result_trans;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult(e.getMessage());
		}
		return ResultBody.createSuccessResult("");
	}

	// 复制库 -创建表定义和物理表
	public ResultBody executeCopyCW_tab(DBInfoBean dbinfo, Map<String, String> tab_map, String centreWarehouseId, String dbInfoId, String userLogId, List<Map<String, Object>> factorList, String Mid,
			String userName) {
		try {
			// 创建表定义
			List<String> sqls = new ArrayList<String>();
			String guid_tab = sqlMapperService.selectOne("select sys_guid() from dual", String.class);
			String sql_inTab = "insert into RAW_CWTABLE (GUID, TABLENAME, TABLECNAME, CENTREWAREHOUSEID, SOURCETABNAME, SOURCETABCNAME, SOURCETABTYPE, DBINFOID,creater,createuser,createuserid) "
					+ " values('" + guid_tab + "', '" + tab_map.get("tableName") + "', '" + tab_map.get("tableCName") + "', '" + centreWarehouseId + "', '" + tab_map.get("TABLENAME") + "', '"
					+ tab_map.get("TABLECNAME") + "', '06', '" + dbInfoId + "','" + userLogId + "','" + userName + "','" + Mid + "' )";
			sqls.add(sql_inTab);
			// 创建字段定义
			for (Map<String, Object> factorMap : factorList) {
				String sql_inFct = "insert into RAW_cwtfactor (TABLEID, COLUMNNAME, DATATYPE, NOTNULL" + ", ANNOTATION"
						+ ", COLUMNCNAME, ISPK, DATA_DEFAULT, DATA_SCALE, DATA_LENGTH,createuser,createuserid,ordernum) " + " values('"
						+ guid_tab
						+ "', '"
						+ factorMap.get("COLUMNNAME")
						+ "', '"
						+ factorMap.get("DATATYPE")
						+ "', '"
						+ factorMap.get("NOTNULL")
						+ "', '"
						+ (factorMap.get("ANNOTATION") == null ? "" : factorMap.get("ANNOTATION"))
						+ "', '"
						+ (factorMap.get("COLUMNCNAME") == null ? "" : factorMap.get("COLUMNCNAME"))
						+ "', '"
						+ factorMap.get("ISPK")
						+ "', '"
						+ (factorMap.get("DATA_DEFAULT") == null ? "" : factorMap.get("DATA_DEFAULT"))
						+ "', '"
						+ (factorMap.get("DATA_SCALE") == null ? "" : factorMap.get("DATA_SCALE"))
						+ "', '"
						+ (factorMap.get("DATA_LENGTH") == null ? "" : factorMap.get("DATA_LENGTH"))
						+ "','"
						+ userName + "','" + Mid + "','" + (factorMap.get("ORDERNUM") == null ? "" : factorMap.get("ORDERNUM")) + "')";
				sqls.add(sql_inFct);
			}
			Integer exeCount = sqlMapperService.execSqls(sqls);
			if (exeCount != null && exeCount == -1) {
				System.out.println(sqls.toString());
				return ResultBody.createErrorResult("创建表定义失败");
			}
			// 创建物理表
			Integer exeCount2 = dwService.generateCWTable(dbinfo, factorList, tab_map.get("tableName").toString(), "0", false);
			if (exeCount2 != null && exeCount2 == -1) {
				return ResultBody.createErrorResult("创建物理表失败");
			}
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("创建表失败<br>" + e.getMessage());
		}
		return ResultBody.createSuccessResult("");
	}

	// 复制贴源库 -执行转换
	@SuppressWarnings("unchecked")
	public ResultBody executeCopyCW_trans(String centreWarehouseId, String oldWarehouseId, DBInfoBean dbinfo, Map<String, String> tab_map, List<Map<String, Object>> factorList, String admdivCode) {
		try {
			Map<String, Object> transInfo = new HashMap<String, Object>();
			// 来源管道
			Map<String, Object> targetPipe_old = this.getTargetPipe(oldWarehouseId);
			DBInfoBean inDBInfoBean = this.getDBInfoBeanByPipeline(targetPipe_old.get("DBTYPE").toString(), targetPipe_old.get("DBINFO").toString());
			inDBInfoBean.DBNAMECN = targetPipe_old.get("PIPELINENAME_REAL").toString();
			inDBInfoBean.DBPWD = AesUtil.aesDecrypt(inDBInfoBean.DBPWD);
			transInfo.put("inDBInfoBean", inDBInfoBean);
			// 目标管道
			dbinfo.DBPWD = AesUtil.aesDecrypt(dbinfo.DBPWD);
			transInfo.put("outDBInfoBean", dbinfo);
			transInfo.put("sName", "贴源库");
			transInfo.put("tName", "贴源库");
			transInfo.put("inTableName", tab_map.get("tableName"));// 表名
			transInfo.put("outTableName", tab_map.get("TABLENAME"));
			transInfo.put("outTableCName", tab_map.get("TABLECNAME"));

			String sFieldNames = "";
			List<InsertUpdateBean> fieldMaps = new ArrayList<InsertUpdateBean>();
			for (Map<String, Object> factorMap : factorList) {
				// 映射关系
				InsertUpdateBean insertUpdateBean = new InsertUpdateBean(factorMap.get("COLUMNNAME").toString(), factorMap.get("COLUMNNAME").toString(), 'Y');
				fieldMaps.add(insertUpdateBean);
				// 设置插入更新依赖字段
				/*
				 * if("1".equals(factorMap.get("ISPK").toString())){
				 * transInfo.put("stream1",
				 * factorMap.get("COLUMNNAME").toString());
				 * transInfo.put("lookup",
				 * factorMap.get("COLUMNNAME").toString()); //设置查询的字段
				 * HashMap<String,String> content = new HashMap<>();
				 * content.put("lookup",
				 * factorMap.get("COLUMNNAME").toString());
				 * content.put("condition", "="); content.put("stream1",
				 * factorMap.get("COLUMNNAME").toString());
				 * content.put("stream2", ""); searchFields.add(content); }
				 */
				sFieldNames += "," + factorMap.get("COLUMNNAME").toString();
			}
			// 设置插入更新依赖字段
			List<HashMap<String, String>> searchFields = new ArrayList<>();
			HashMap<String, String> content = new HashMap<>();
			content.put("lookup", "BID");
			content.put("condition", "=");
			content.put("stream1", "BID");
			content.put("stream2", "");
			searchFields.add(content);
			transInfo.put("searchFields", searchFields);
			// sFieldNames = sFieldNames.substring(0, sFieldNames.length()-1);
			String inSql = "select bid" + sFieldNames + " from " + tab_map.get("TABLENAME");
			transInfo.put("fieldMaps", fieldMaps);
			transInfo.put("inSql", inSql);
			// 转换目录
			Map<String, Object> cw_map = sqlMapperService.selectOne("select CNAME,DATACATEGORYID from raw_centrewarehouse where GUID='" + centreWarehouseId + "'");
			String transDrc = EtlConstant.SYSDIRECTORY + "/" + EtlConstant.SOURCE_TYK + "";
			if (cw_map.get("DATACATEGORYID") != null) {
				transDrc += rawService.getRawDirectories(cw_map.get("DATACATEGORYID").toString());
			}
			transDrc += ("/" + cw_map.get("CNAME").toString());
			transInfo.put("transDrc", transDrc);

			TransMetaConfig transMetaCon = setTransMetaCfg(transInfo, "raw");
			// 生成转换
			ResultBody res = packageService.createTrans(admdivCode, transMetaCon);

			if (res.isError) {
				return ResultBody.createErrorResult("插入数据失败!<br>" + res.errMsg);
			} else {
				Map<String, Object> map_trans = (Map<String, Object>) res.result;
				// 执行转换
				Integer transId = Integer.valueOf(map_trans.get("transId").toString());
				packageService.runTrans2(admdivCode, transId);

				// result = ResultBody.createSuccessResult("");
			}
			return ResultBody.createSuccessResult("");
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("系统错误<br>" + e.getMessage());
		}
	}

	/**
	 * 复制库 -- - 生成库 - 从原有库复制表定义 - 在其选择管道重新创建物理表
	 * 
	 * @param centreWarehouseId
	 * @return
	 */
	/*
	 * @ResponseBody
	 * 
	 * @RequestMapping("/executeCopyCW") public ResultBody
	 * executeCopyCW(HttpServletRequest request) { ResultBody result = null;
	 * try{ Map<String, Object> transInfo = new HashMap<String, Object>();//
	 * 抽取数据使用 Map<String, Object> dlsInfo = new HashMap<String, Object>();//
	 * 血缘关系使用 String centreWarehouseId =
	 * request.getParameter("centreWarehouseId"); //校验目标管道连接 Map<String, Object>
	 * targetPipe = this.getTargetPipe(centreWarehouseId); if(targetPipe ==
	 * null) return ResultBody.createErrorResult("目标数据管道信息获取失败，请检查！");
	 * DBInfoBean dbinfo =
	 * this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").
	 * toString(),targetPipe.get("DBINFO").toString()); boolean testConn =
	 * dynamicConnServiceV2.testConn(dbinfo); if(!testConn) return
	 * ResultBody.createErrorResult("目标数据管道连接失败，请检查！"); // 来源管道放入转换配置信息 String
	 * oldWarehouseId = request.getParameter("oldWarehouseId"); String buildType
	 * = request.getParameter("buildType"); // 构建类型 0 ：仅结构 1：结构和数据 Map<String,
	 * Object> targetPipe_old = this.getTargetPipe(oldWarehouseId); DBInfoBean
	 * inDBInfoBean =
	 * this.getDBInfoBeanByPipeline(targetPipe_old.get("DBTYPE").toString
	 * (),targetPipe_old.get("DBINFO").toString()); inDBInfoBean.DBNAMECN =
	 * targetPipe_old.get("PIPELINENAME_REAL").toString(); inDBInfoBean.DBPWD =
	 * AesUtil.aesDecrypt(inDBInfoBean.DBPWD); transInfo.put("inDBInfoBean",
	 * inDBInfoBean); transInfo.put("sName", "贴源库"); transInfo.put("tName",
	 * "贴源库");
	 * 
	 * UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
	 * String row = request.getParameter("row"); Map<String,Object> map =
	 * JSONObject.fromObject(row);
	 * 
	 * transInfo.put("inTableName", map.get("tableName"));// 表名
	 * transInfo.put("outTableName", map.get("TABLENAME"));
	 * transInfo.put("outTableCName", map.get("TABLECNAME"));
	 * dlsInfo.put("sTableId", map.get("TABLENAME")); dlsInfo.put("tTableId",
	 * map.get("tableName")); dlsInfo.put("sTableName", map.get("TABLECNAME"));
	 * dlsInfo.put("tTableName", map.get("tableCName"));
	 * dlsInfo.put("FILTER_CONDI", ""); dlsInfo.put("FILTER_DESC", "");
	 * dlsInfo.put("sAppid", "RAW"); dlsInfo.put("sAppName", "贴源库系统");
	 * dlsInfo.put("sDatabaseid", "YCD_MFBI"); dlsInfo.put("sDatabaseName",
	 * "元数据"); dlsInfo.put("tAppid", "RAW"); dlsInfo.put("tAppName", "贴源库系统");
	 * dlsInfo.put("tDatabaseid", "YCD_MFBI"); dlsInfo.put("tDatabaseName",
	 * "元数据");
	 * 
	 * List<String> sqls = new ArrayList<String>(); // 创建表定义 String sql_guid =
	 * "select sys_guid() from dual"; String guid_tab =
	 * sqlMapperService.selectOne(sql_guid,String.class); String sql_inTab =
	 * "insert into RAW_CWTABLE (GUID, TABLENAME, TABLECNAME, CENTREWAREHOUSEID, SOURCETABNAME, SOURCETABCNAME, SOURCETABTYPE, DBINFOID,creater) "
	 * +
	 * " values('"+guid_tab+"', '"+map.get("tableName")+"', '"+map.get("tableCName"
	 * )+"', '"+centreWarehouseId+"', '"+map.get("TABLENAME")+"', '"+map.get(
	 * "TABLECNAME")+"', '06', '"+targetPipe.get("GUID")+"','"+user.LogID+"' )";
	 * sqls.add(sql_inTab);
	 * 
	 * String sFieldNames = ""; List<InsertUpdateBean> fieldMaps = new
	 * ArrayList<InsertUpdateBean>(); // 创建表字段定义 String columnNames = "";
	 * if(map.get("columnNames") != null){ columnNames = " and COLUMNNAME in(" +
	 * map.get("columnNames").toString() + ")"; } String sql_seFactor =
	 * "select * from RAW_cwtfactor where TABLEID='" + map.get("GUID") + "' " +
	 * columnNames; List<Map<String, Object>> factorList =
	 * sqlMapperService.selectList(sql_seFactor); for (Map<String, Object>
	 * factorMap : factorList) { // 映射关系 InsertUpdateBean insertUpdateBean = new
	 * InsertUpdateBean(factorMap.get("COLUMNNAME").toString(),
	 * factorMap.get("COLUMNNAME").toString(), 'Y');
	 * fieldMaps.add(insertUpdateBean); // 设置插入更新依赖字段 List<HashMap<String,
	 * String>> searchFields = new ArrayList<>();
	 * if("1".equals(factorMap.get("ISPK").toString())){
	 * transInfo.put("stream1", factorMap.get("COLUMNNAME").toString());
	 * transInfo.put("lookup", factorMap.get("COLUMNNAME").toString());
	 * //设置查询的字段 HashMap<String,String> content = new HashMap<>();
	 * content.put("lookup", factorMap.get("COLUMNNAME").toString());
	 * content.put("condition", "="); content.put("stream1",
	 * factorMap.get("COLUMNNAME").toString()); content.put("stream2", "");
	 * searchFields.add(content); } transInfo.put("searchFields", searchFields);
	 * sFieldNames += factorMap.get("COLUMNNAME").toString() + ",";
	 * 
	 * String ANNOTATION = ""; if(factorMap.get("ANNOTATION") != null){
	 * ANNOTATION = factorMap.get("ANNOTATION").toString(); } String COLUMNCNAME
	 * = ""; if(factorMap.get("COLUMNCNAME") != null){ COLUMNCNAME =
	 * factorMap.get("COLUMNCNAME").toString(); } String DATA_DEFAULT = "";
	 * if(factorMap.get("DATA_DEFAULT") != null){ DATA_DEFAULT =
	 * factorMap.get("DATA_DEFAULT").toString(); } int DATA_SCALE = 0;
	 * if(factorMap.get("DATA_SCALE") != null){ DATA_SCALE =
	 * Integer.parseInt(factorMap.get("DATA_SCALE").toString()); } String
	 * sql_inFct =
	 * "insert into RAW_cwtfactor (COLUMNNAME, DATATYPE, NOTNULL, ANNOTATION, TABLEID, COLUMNCNAME, ISPK, DATA_DEFAULT, DATA_SCALE, DATA_LENGTH) "
	 * +
	 * " values('"+factorMap.get("COLUMNNAME")+"', '"+factorMap.get("DATATYPE")
	 * +"', '"
	 * +factorMap.get("NOTNULL")+"', '"+ANNOTATION+"', '"+guid_tab+"', '"+
	 * COLUMNCNAME
	 * +"', '"+factorMap.get("ISPK")+"', '"+DATA_DEFAULT+"', '"+DATA_SCALE
	 * +"', "+factorMap.get("DATA_LENGTH")+")"; sqls.add(sql_inFct); }
	 * sFieldNames = sFieldNames.substring(0, sFieldNames.length()-1); String
	 * inSql = "select " + sFieldNames + " from " + map.get("TABLENAME");
	 * transInfo.put("fieldMaps", fieldMaps); transInfo.put("inSql", inSql);
	 * sqlMapperService.execSqls(sqls); // 创建物理表 false表中是否存在数据
	 * dwService.generateCWTable(dbinfo, factorList,
	 * map.get("tableName").toString(),"0",false); Integer transId = -1;
	 * if("1".equals(buildType)){ dbinfo.DBPWD =
	 * AesUtil.aesDecrypt(dbinfo.DBPWD); transInfo.put("outDBInfoBean", dbinfo);
	 * 
	 * TransMetaConfig transMetaCon = setTransMetaCfg(transInfo); // 生成转换
	 * ResultBody res = packageService.createTrans(user.optAdmdivCode,
	 * transMetaCon);
	 * 
	 * if(res.isError){ return
	 * ResultBody.createErrorResult("插入数据失败!<br>"+res.errMsg); }else{
	 * Map<String,Object> map_trans = (Map<String,Object>)res.result; // 执行转换
	 * transId = Integer.valueOf(map_trans.get("transId").toString()); result =
	 * ResultBody.createSuccessResult(transId); // result =
	 * packageService.runTrans(transId); }
	 * 
	 * // 创建血缘关系 dlsInfo.put("TRANSID", transId); ResultBody dlsResult =
	 * dlsService.createDls(dlsInfo); if(dlsResult.isError){ return
	 * ResultBody.createErrorResult("创建血缘关系失败！<br>"+dlsResult.errMsg); } }
	 * return ResultBody.createSuccessResult(transId); }catch (Exception e){
	 * e.printStackTrace(); result =
	 * ResultBody.createErrorResult("系统错误<br>"+e.getMessage()); } return result;
	 * }
	 */
	/**
	 * 从贴源库生成资源目录
	 * 
	 * @param request
	 * @return
	 * @author HRK
	 * @date 2019年4月28日
	 */
	@ResponseBody
	@RequestMapping("/generateDrcByRAW")
	public ResultBody generateDrcByRAW(HttpServletRequest request) {
		UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
		String admdivId = user.optAdmdiv;
		String admdivCode = user.optAdmdivCode;
		try {
			String params = request.getParameter("params");
			return mdmService.generateDrcByRAW(params, "贴源库", admdivId, admdivCode);
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("操作失败！" + e.getMessage());
		}
	}

	/**
	 * 从贴源库生成主数据 获取主数据目录
	 */
	@ResponseBody
	@RequestMapping("/getModelListTypes")
	public ResultBody getModelListTypes(HttpServletRequest request) {
		UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
		try {
			return mdmService.getModelListTypes(user.optAdmdivCode);
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("操作失败！");
		}

	}

	/**
	 * 从贴源库生成主数据
	 * 
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/generateMDMByRAW")
	public ResultBody generateMDMByRAW(HttpServletRequest request) {
		ResultBody result = null;
		try {
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
			String row = request.getParameter("row");
			String businessType = request.getParameter("businessType");
			Map<String, String> map = new HashMap<String, String>();
			map.put("ADMIDV", user.optAdmdiv);
			map.put("ADMDIVCODE", user.optAdmdivCode);
			// 生成主数据
			result = mdmService.generateMDMByRAW(row, businessType, map);
			if (result.isError) {
				return ResultBody.createErrorResult("生成主数据失败!<br>" + result.errMsg);
			} else {
				Map<String, Object> param_map = JSONObject.fromObject(row);
				// 是否创建转换 -数据抽取
				if ("1".equals(param_map.get("ISCREATETRANS").toString())) {
					// 抽取数据
					Map<String, Object> transInfo = (Map<String, Object>) result.result;
					String centreWarehouseId = request.getParameter("centreWarehouseId");
					Map<String, Object> dataMap = sqlMapperService
							.selectOne("select t1.dbinfo, t2.pipelinename from BI_T_DBINFO t1, bi_datapipeline t2 where t1.pipelineid = t2.guid and centrewarehouseid='" + centreWarehouseId + "'");

					DBInfoBean inDBInfoBean = getDBInfoBeanByPipeline("ORACLE", dataMap.get("DBINFO").toString());
					inDBInfoBean.DBPWD = AesUtil.aesDecrypt(inDBInfoBean.DBPWD);
					transInfo.put("inDBInfoBean", inDBInfoBean);
					transInfo.put("sName", "贴源库");
					transInfo.put("tName", "主数据");
					transInfo.put("transDrc", "数据中心/主数据");// 数据抽取目录
					TransMetaConfig transMetaCon = setTransMetaCfg(transInfo, "raw_mdm");
					transMetaCon.setDataSourceName(inDBInfoBean.DBNAMECN);
					// 生成转换
					ResultBody res = packageService.createTrans(user.optAdmdivCode, transMetaCon);
					Integer transId = -1;
					if (res.isError) {
						return ResultBody.createErrorResult("插入数据失败!<br>" + res.errMsg);
					} else {
						Map<String, Object> _map = (Map<String, Object>) res.result;
						// 执行转换
						transId = Integer.valueOf(_map.get("transId").toString());
						result = ResultBody.createSuccessResult(transId);
						// result = packageService.runTrans(transId);
					}

					// 创建血缘关系
					Map<String, Object> dlsInfo = (Map<String, Object>) transInfo.get("dlsInfo");
					dlsInfo.put("TRANSID", transId);
					dlsInfo.put("sAppid", "RAW");
					dlsInfo.put("sAppName", "贴源库系统");
					dlsInfo.put("sDatabaseid", "YCD_MFBI");
					dlsInfo.put("sDatabaseName", "元数据");
					dlsInfo.put("tAppid", "MDM");
					dlsInfo.put("tAppName", "主数据系统");
					dlsInfo.put("tDatabaseid", "YCD_MDM");
					dlsInfo.put("tDatabaseName", "主数据");
					ResultBody dlsResult = dlsService.createDls(dlsInfo);
					if (dlsResult.isError) {
						return ResultBody.createErrorResult("创建血缘关系失败！<br>" + dlsResult.errMsg);
					}
				} else {
					result = ResultBody.createSuccessResult("");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			result = ResultBody.createErrorResult("操作失败！ " + e.getMessage());
		}
		return result;
	}

	// 数据抽取配置信息 --贴源库生成主数据、贴源库复制库 共用
	public TransMetaConfig setTransMetaCfg(Map<String, Object> transInfo, String type) {
		String _name = transInfo.get("outTableCName").toString() + "(" + transInfo.get("outTableName").toString() + ")";
		TransMetaConfig transMetaCon = new TransMetaConfig(_name, transInfo.get("transDrc").toString());
		// 为TransMetaConfig对象配置数据库链接集合
		Map<String, DBInfoBean> conns = new HashMap<>();
		conns.put("inDBInfoBean", (DBInfoBean) transInfo.get("inDBInfoBean"));
		conns.put("outDBInfoBean", (DBInfoBean) transInfo.get("outDBInfoBean"));
		transMetaCon.setConns(conns);
		// 数据来源
		transMetaCon.setDataSource(EtlConstant.SOURCE_TYK);
		// 为TransMetaConfig对象配置BaseStepConfig集合
		// 步骤列表集合
		List<BaseStepConfig> steps = new ArrayList<>();
		// 表输入
		// 实例化对象
		// -- 映射关系
		List<InsertUpdateBean> fieldMaps = (List<InsertUpdateBean>) transInfo.get("fieldMaps");
		TableInputConfig inputStepconfig = new TableInputConfig(transInfo.get("inSql").toString(), "inDBInfoBean");
		// 设置步骤名称
		inputStepconfig.setStepName("来源表表输入");
		// 设置步骤描述
		inputStepconfig.setDescription("数据来源：" + transInfo.get("sName").toString() + "\r\n表名：" + transInfo.get("inTableName").toString());
		// 设置坐标
		inputStepconfig.setLeft(150);
		inputStepconfig.setTop(100);
		steps.add(inputStepconfig);
		// 设置下一步骤
		ArrayList<String> nextSteps = new ArrayList<>();
		nextSteps.add("表输出");
		inputStepconfig.setNextStepName(nextSteps);
		/*
		 * List<HashMap<String, String>> searchFields = new ArrayList<>();
		 * HashMap<String,String> content = new HashMap<>();
		 * content.put("lookup", transInfo.get("lookup").toString());// 目标字段名
		 * content.put("condition", "="); content.put("stream1",
		 * transInfo.get("stream1").toString()); content.put("stream2", "");
		 * searchFields.add(content);
		 */
		// 表输出
		// 实例化表输出对象
		InsertUpdateConfig inOrUpStepconfig = new InsertUpdateConfig("outDBInfoBean", transInfo.get("outTableName").toString(), fieldMaps,
				(List<HashMap<String, String>>) transInfo.get("searchFields"));
		// 设置步骤名称
		inOrUpStepconfig.setStepName("表输出");
		// 设置步骤描述
		inOrUpStepconfig.setDescription("输出目标：" + transInfo.get("tName").toString() + "\r\n表名：" + transInfo.get("inTableName").toString());
		// 设置坐标
		inOrUpStepconfig.setLeft(250);
		inOrUpStepconfig.setTop(100);
		steps.add(inOrUpStepconfig);
		if (!"raw".equals(type)) {
			// 设置下一步骤 -阻塞数据
			ArrayList<String> nextStep_zesj = new ArrayList<>();
			nextStep_zesj.add("阻塞数据");
			inOrUpStepconfig.setNextStepName(nextStep_zesj);

			// 阻塞数据
			BlockDataConfig blockData = new BlockDataConfig("阻塞数据");
			blockData.setPassAllRows(true);// 阻止全部
			blockData.setPrefix("block");// 前缀
			blockData.setCacheSize(5000);// 缓存大小
			blockData.setCompressFiles(true);// 是否压缩文件
			// 设置坐标、加入步骤集合
			blockData.setLeft(350);
			blockData.setTop(100);
			steps.add(blockData);

			// 设置下一步骤
			ArrayList<String> nextSteps_sx = new ArrayList<>();
			nextSteps_sx.add("刷新superid");
			blockData.setNextStepName(nextSteps_sx);

			String sql_update = "UPDATE " + transInfo.get("outTableName").toString() + " set SUPERID='#'" + " where versionno='" + transInfo.get("versionNo").toString() + "' and "
					+ " (SUPERID is null or SUPERID not in (select REFID from " + transInfo.get("outTableName").toString() + "));" + "\r\n call REF_SP_REFRESHALLLVLID('"
					+ transInfo.get("outTableName").toString() + "','#'" + ",'" + transInfo.get("versionNo").toString() + "')";
			ExecSQLConfig execsql = new ExecSQLConfig("outDBInfoBean", sql_update);
			execsql.setStepName("刷新superid");
			// 设置执行每一行以及变量替换
			execsql.setExecuteEachInputRow('Y');
			// 设置作为参数的字段 -必填
			List<String> arguments = new ArrayList<>();
			// 设置坐标、加入步骤集合
			execsql.setArguments(arguments);
			execsql.setLeft(450);
			execsql.setTop(100);
			steps.add(execsql);
		}

		// 把步骤列表集合添加到转换对象
		transMetaCon.setSteps(steps);
		return transMetaCon;
	}

	/**
	 * 从贴源库生成主数据 获取物理表名 进行校验
	 * 
	 * @return
	 * @author HRK
	 * @date 2019年5月10日
	 */
	@ResponseBody
	@RequestMapping("/getRelaTableName")
	public ResultBody getRelaTableName(String businessType) {
		try {
			return mdmService.getRelaTableName(businessType);
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("操作失败！");
		}
	}

	/**
	 * 根据资源目录分类获取表名 -贴源库生成资源目录 表名重复校验
	 * 
	 * @param request
	 * @return
	 * @author HRK
	 * @date 2019年5月15日
	 */
	@ResponseBody
	@RequestMapping("/getModelNamesByType")
	public ResultBody getModelNamesByType(String resourcetypeid) {
		try {
			return mdmService.getModelNamesByType(resourcetypeid);
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("操作失败！");
		}
	}

	/**
	 * 获取主数据新增表模板字段 -从贴源库创建主数据
	 * 
	 * @return
	 * @author HRK
	 * @date 2019年5月17日
	 */
	@ResponseBody
	@RequestMapping("/getModelFieldData")
	public ResultBody getModelFieldData() {
		ResultBody result = null;
		try {
			result = mdmService.getModelFieldData();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			result = ResultBody.createErrorResult("系统错误！" + e.getMessage());
		}
		return result;
	}

	/**
	 * 贴源库表来源统计
	 * 
	 * @param request
	 * @param dataCategoryId
	 *            数据分类id
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getGuidCount")
	public List<Map<String, Object>> getGuidCount(HttpServletRequest request, String guid) {
		try {
			return sqlMapperService.selectList("SELECT count(guid) as count,t.sourcetabtype FROM raw_cwtable t where t.centrewarehouseid='" + guid + "' group by t.sourcetabtype");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 根据仓库表id获取字段信息
	 * 
	 * @param tableId
	 *            贴源库仓库表id
	 * @return
	 * @author HRK
	 * @date 2019年6月4日
	 */
	@RequestMapping("getCWTFactor")
	@ResponseBody
	public ResultBody getCWTFactor(String tableId) {
		ResultBody result = null;
		try {
			String sql = "select * from raw_cwtfactor where tableid='" + tableId + "' order by ordernum,columnname";
			List<Map<String, Object>> pipelineid = sqlMapperService.selectList(sql);
			result = ResultBody.createSuccessResult(pipelineid);
		} catch (Exception e) {
			e.printStackTrace();
			result = ResultBody.createErrorResult(e.getMessage());
		}
		return result;
	}

	/**
	 * 批量执行转换
	 * 
	 * @param transIds
	 *            转换id集合
	 * @author HRK
	 * @date 2019年6月10日
	 */
	@ResponseBody
	@RequestMapping("/ctrateTrans")
	public void ctrateTrans(HttpServletRequest request) {
		UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
		String transIds = request.getParameter("transIds");
		try {
			@SuppressWarnings("unchecked")
			List<Integer> transIds_ = JSONArray.fromObject(transIds);
			for (Integer transId : transIds_) {
				packageService.runTrans2(user.optAdmdivCode, transId);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据分类id获取贴源库名称 创建贴源库时校验名称
	 * 
	 * @param datacategoryid
	 *            分类id
	 * @return
	 * @author HRK
	 * @date 2019年6月14日
	 */
	@ResponseBody
	@RequestMapping("getCWByCategoryId")
	public List<String> getCWByCategoryId(String datacategoryid) {
		try {
			String sql = "select cname from raw_centrewarehouse where datacategoryid='" + datacategoryid + "'";
			List<String> cNames = sqlMapperService.selectList(sql, String.class);
			return cNames;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	/**
	 * 根据资源目录/主数据批量建表，获取当前贴源库已经使用过的资源目录/主数据
	 * 
	 * @param centreWarehouseId
	 * @param _type
	 *            04：资源目录 05：主数据
	 * @return
	 */
	@ResponseBody
	@RequestMapping("getExistsDRCversionids")
	public List<String> getExistsDRCversionids(String centreWarehouseId, String _type) {
		try {
			String sql = "select DRC_VERSIONID from RAW_CWTABLE where SOURCETABTYPE='" + _type + "' and DRC_VERSIONID is not null and centreWarehouseId='" + centreWarehouseId + "'";
			return sqlMapperService.selectList(sql, String.class);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	@ResponseBody
	@RequestMapping("getColumns")
	public ResultBody getColumns(String dataPipelineId, String _type, String row, String sourceTabType) {
		Map<String, Object> pipeline = this.getPipeline(dataPipelineId);
		if (pipeline == null) {
			return ResultBody.createErrorResult("业务库数据管道信息获取失败，请检查！");
		}
		DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
		try {
			dynamicConnServiceV2.testConn(dbinfo);
		} catch (Exception e) {
			return ResultBody.createErrorResult("业务库数据管道连接失败，请检查管道信息是否正确！");
		}
		Map<String, Object> row_table = JSONObject.fromObject(row);
		List<Map<String, Object>> columns = new ArrayList<>();
		if ("02".equals(sourceTabType)) {
			columns = getCWFactorsList_2_0(dbinfo, row_table.get("TABLE_NAME").toString(), row_table.get("_columns"));
		} else {
			columns = this.getCWFactorsList(dbinfo, row_table.get("TABLE_NAME").toString(), row_table.get("_columns"));
		}
		return ResultBody.createSuccessResult(columns);
	}

	// 新增/修改贴源库分类 -修改分类名称
	@ResponseBody
	@RequestMapping("addOrEditCwTableCatName")
	public ResultBody addOrEditCwTableCatName(String centreWarehouseId, String catId, String catName, String type) {
		return dwService.addOrEditCwTableCatName(centreWarehouseId, catId, catName, type);
	}

	// 删除贴源库分类
	@ResponseBody
	@RequestMapping("deleteCwTableCat")
	public ResultBody deleteCwTableCat(String catId, String centreWarehouseId) {
		try {
			List<String> sqls = new ArrayList<String>();
			sqls.add("delete from RAW_CWTABLECAT where guid='" + catId + "' and CENTREWAREHOUSEID='" + centreWarehouseId + "'");
			sqls.add("update RAW_CWTABLE set CATID='default' where CATID='" + catId + "'");
			sqlMapperService.execSqls(sqls);
			return ResultBody.createSuccessResult("");
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult(e.getMessage());
		}
	}

	// 修改贴源库表的分类
	@ResponseBody
	@RequestMapping("editCwTable_catId")
	public ResultBody editCwTable_catId(String pid_n, String tids) {
		try {
			String sql = "update RAW_CWTABLE set catid='" + pid_n + "' where guid in(" + tids + ")";
			sqlMapperService.update(sql);
			return ResultBody.createSuccessResult("");
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult(e.getMessage());
		}
	}

	// 修改贴源库表分类的顺序
	@ResponseBody
	@RequestMapping("editCwTableCatOrder")
	public ResultBody editCwTableCatOrder(String centreWarehouseId, String catId_s, String catId_t) {
		List<String> sqls = new ArrayList<String>();
		try {
			// 所有目标分类后面的分类 ordernum+1
			String sql1 = "update RAW_CWTABLECAT set ORDERNUM = ORDERNUM+1 " + " where ORDERNUM > (select t.ORDERNUM from RAW_CWTABLECAT t where t.GUID='" + catId_t + "' and t.CENTREWAREHOUSEID='"
					+ centreWarehouseId + "') " + " and CENTREWAREHOUSEID='" + centreWarehouseId + "'";
			// 拖动分类的ordernum 改为目标分类ordernum+1
			String sql2 = "update RAW_CWTABLECAT set ORDERNUM = " + "(select t.ORDERNUM+1 from RAW_CWTABLECAT t where t.GUID='" + catId_t + "' and t.CENTREWAREHOUSEID='" + centreWarehouseId + "')"
					+ "where CENTREWAREHOUSEID='" + centreWarehouseId + "' and GUID='" + catId_s + "'";
			sqls.add(sql1);
			sqls.add(sql2);
			sqlMapperService.execSqls(sqls);
			return ResultBody.createSuccessResult("");
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult(e.getMessage());
		}
	}

	/**
	 * 删除
	 * 
	 * @param request
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/deleteCDWCols")
	public ResultBody deleteCDWCols(HttpServletRequest request) {
		String centreWarehouseId = request.getParameter("centreWarehouseId");
		Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId); // 仓库目标管道
		if (targetPipe == null) {
			return ResultBody.createErrorResult("目标数据管道信息获取失败，请检查！");
		}

		DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());
		try {
			dynamicConnServiceV2.testConn(dbinfo);
		} catch (Exception e) {
			return ResultBody.createErrorResult("目标数据管道连接失败，请检查！");
		}

		String tableId = request.getParameter("tableId");// 表id
		String tableName = request.getParameter("tableName"); // 表物理名称
		String tableCName = request.getParameter("tableCName"); // 表中文名称
		String factorInfoRows = request.getParameter("factorInfoRows"); // 增加或修改表的所有列信息（rows）
		String colid = request.getParameter("colid"); // 删除列id
		String colName = request.getParameter("colName"); // 删除列名
		String sql = "";
		String editKey = request.getParameter("editKey");
		String sourceType = request.getParameter("sourceType");

		sql = "delete from RAW_cwtfactor where columnname ='" + colName + "' and TABLEID='" + tableId + "' and guid = '" + colid + "'"; // 先删除当前表的列信息
		String IS_VIEW = sqlMapperService.selectOne("select IS_VIEW from RAW_cwtable where guid = '" + tableId + "'", String.class);
		// 列信息维护
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> facts = JSONArray.fromObject(factorInfoRows);

		if (!"02".equals(sourceType) && !"1".equals(IS_VIEW)) {
			ResultBody createCDWTable = this.createCDWTable(dbinfo, tableName, tableCName, facts, editKey);
			if (createCDWTable.isError) {
				return createCDWTable;
			}
		}
		// 构建物理表
		// 修改贴源库定义
		sqlMapperService.execSql(sql);

		Map<String, Object> resMap = new HashMap<String, Object>();
		resMap.put("guid", tableId);
		resMap.put("msg", "删除成功");
		return ResultBody.createSuccessResult(resMap);
	}

	// 贴源库表【手工录入】查看表定义
	@ResponseBody
	@RequestMapping("/searchTableData_getCol")
	public ResultBody searchTableData_getCol(String centreWarehouseId, String tableId) {
		// 获取仓库管道
		Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId);
		DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());
		return rawService.searchTableData_getCol(dbinfo, tableId);
	}

	// 贴源库表【手工录入】 、中心仓库【手工录入】 查看数据
	@ResponseBody
	@RequestMapping(value = "/searchTableData_getData", method = RequestMethod.POST)
	public PageBean<Map<String, Object>> searchTableData_getData(HttpServletRequest request, String centreWarehouseId, String tableName, String fields, String filter, String page, String rows,
			String sidx, String sord, String tables) {
		try {
			int start = 0, end = 0;
			if (page != null && rows != null) {
				int p = Integer.parseInt(page);
				int rs = Integer.parseInt(rows);
				start = (p - 1) * rs + 1;
				end = p * rs;
			}
			// 获取仓库管道
			Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId);
			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());

			String order = "";
			if (sidx != null && !"".equals(sidx)) {
				order = " order by " + sidx + " " + sord;
			}
			if (DBTypeConstant.MPPDB.equals(dbinfo.DBTYPE)) {
				fields += "t.dwtab_datacollection,t.dwtab_last_update_time";
			} else {
				fields += ",t.DWTAB_DATACOLLECTION,TO_CHAR(T.DWTAB_LAST_UPDATE_TIME, 'yyyy-mm-dd hh24:mi:ss') DWTAB_LAST_UPDATE_TIME";
			}
			String sql = "select " + fields + " from " + tableName + " t ";
			String where = " where 1=1 ";
			if (tables != null) {
				sql += tables;
			}
			String sqlPage = "";
			if(filter != null && !"".equals(filter)){
				where += " and "+filter;
				sql += where;
			}
			if (DBTypeConstant.MPPDB.equals(dbinfo.DBTYPE)) {
				if (sidx != null && !"".equals(sidx)) {
					sqlPage = sql +" order by t.DWTAB_DATACOLLECTION," + sidx + " " + sord + " limit " + rows + " offset " + start;
				} else {
					sqlPage = sql + " order by t.DWTAB_DATACOLLECTION limit " + rows + " offset " + (start - 1);
				}
			} else if("GPDB".equals(dbinfo.DBTYPE)){
				sqlPage = "select * from (select row_number() over() r,tt.* from (" + sql+ ") tt " + ") a where r <= "+end+" and r >=" + start
						+ order;
			} else{
				//String where = " where t.DWTAB_DATACOLLECTION='02' "; // 手工录入数据标识 02
				/*where = " where 1=1 ";
				if (filter != null && !"".equals(filter))
					where += " and " + filter;
				sql += where;*/
				sqlPage = "select * from (select rownum r,tt.* from (" + sql + ") tt " + " where rownum <=" + end + " ) where r >=" + start + order;
			}

			List<Map<String, Object>> dataList = dynamicConnServiceV2.selectList(dbinfo, sqlPage);

			Integer count = dynamicConnServiceV2.selectOne(dbinfo, "select count(1) from (" + sql + ") tt", Integer.class);

			PageBean<Map<String, Object>> pageBean = new PageBean<Map<String, Object>>();
			pageBean.setRows(dataList);
			count = (count == null ? 0 : count);
			pageBean.setTotal(count);
			BigDecimal bi1 = new BigDecimal(count.toString());
			BigDecimal bi2 = new BigDecimal(rows);
			BigDecimal bi3 = bi1.divide(bi2, 0, RoundingMode.UP);
			double totalPage = bi3.doubleValue();
			pageBean.setTotalPage((int) Math.ceil(totalPage));
			return pageBean;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	// 专题集市【手工录入】 查看数据
	@ResponseBody
	@RequestMapping(value = "/searchDMTableData_getData", method = RequestMethod.POST)
	public PageBean<Map<String, Object>> searchDMTableData_getData(HttpServletRequest request, String centreWarehouseId, String tableName, String fields, String filter, String page, String rows,
			String sidx, String sord, String tables) {
		try {
			int start = 0, end = 0;
			if (page != null && rows != null) {
				int p = Integer.parseInt(page);
				int rs = Integer.parseInt(rows);
				start = (p - 1) * rs + 1;
				end = p * rs;
			}
			// 获取仓库管道
			Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId);
			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());

			String order = "";
			if (sidx != null && !"".equals(sidx)) {
				order = " order by " + sidx + " " + sord;
			}
			if (DBTypeConstant.MPPDB.equals(dbinfo.DBTYPE)) {
				fields += ",t.from_unixtime(unix_timestamp(t.dwtab_last_update_time),'yyyy-MM-dd HH:mm:ss') as dwtab_last_update_time";
			} else {
				fields += ",to_char(t.DWTAB_LAST_UPDATE_TIME,'yyyy-mm-dd hh24:mi:ss') as DWTAB_LAST_UPDATE_TIME";
			}
			String sql = "select " + fields + " from " + tableName + " t ";
			if (tables != null) {
				sql += tables;
			}
			String sqlPage = "";
			String where = " where 1=1 ";
			if (filter != null && !"".equals(filter)){
				where += " and " + filter;
				sql  += where;
			}
			if (DBTypeConstant.MPPDB.equals(dbinfo.DBTYPE)) {
				if (sidx != null && !"".equals(sidx)) {
					sqlPage = sql +  sidx + " " + sord + " limit " + rows + " offset " + start;
				} else {
					sqlPage = sql  + " limit " + rows + " offset " + (start - 1);
				}
			} else if("GPDB".equals(dbinfo.DBTYPE)){
				sqlPage = "select * from (select row_number() over() r,tt.* from (" + sql +  ") tt  ) a where r<= "+end+" and r >=" + start + order;
			} else{
				//String where = " where t.DWTAB_DATACOLLECTION='02' "; // 手工录入数据标识 02
				sqlPage = "select * from (select rownum r,tt.* from (" + sql + ") tt " + " where rownum <=" + end + " ) where r >=" + start + order;
			}

			List<Map<String, Object>> dataList = dynamicConnServiceV2.selectList(dbinfo, sqlPage);

			Integer count = dynamicConnServiceV2.selectOne(dbinfo, "select count(1) from (" + sql + ") d", Integer.class);

			PageBean<Map<String, Object>> pageBean = new PageBean<Map<String, Object>>();
			pageBean.setRows(dataList);
			count = (count == null ? 0 : count);
			pageBean.setTotal(count);
			BigDecimal bi1 = new BigDecimal(count.toString());
			BigDecimal bi2 = new BigDecimal(rows);
			BigDecimal bi3 = bi1.divide(bi2, 0, RoundingMode.UP);
			double totalPage = bi3.doubleValue();
			pageBean.setTotalPage((int) Math.ceil(totalPage));
			return pageBean;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	// 贴源库-手工录入-保存数据
	@ResponseBody
	@RequestMapping("saveRawTabData")
	public ResultBody saveRawTabData(String type, String tableName, String centreWarehouseId, String columnObjs, String bid) {
		// 获取仓库管道
		Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId);
		DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());
		return rawService.saveRawTabData(dbinfo, type, tableName, centreWarehouseId,columnObjs, bid);
	}

	// 贴源库-手工录入-爬虫采集-保存数据
	@ResponseBody
	@RequestMapping("saveRawTabDataBySelenium")
	public ResultBody saveRawTabDataBySelenium(String type, String tableName, String centreWarehouseId, String columnObjs, String bid, String columnObjsXpath) throws IOException, InterruptedException {
		//获取多条columnObjs数据
//		List<Map<String,Object>> col_list_xpath = JSONArray.fromObject(columnObjsXpath);
//		seleniumGetData("http://www.jurong.gov.cn/jurong/tongji/202009/d3323359c321435cad9e0419297e9fe4.shtml",
//				(String) col_list_xpath.get(0).get("xpath"));
		// 获取仓库管道
		Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId);
		DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());
		return rawService.saveRawTabData(dbinfo, type, tableName, centreWarehouseId,columnObjs, bid);
	}

	// 贴源库 -手工录入 -删除数据
	@ResponseBody
	@RequestMapping("delRawTabData")
	public ResultBody delRawTabData(String centreWarehouseId, String tableName, String bid) {
		ResultBody result = ResultBody.createSuccessResult("");
		try {
			// 获取仓库管道
			Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId);
			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());

			String sql = "delete from " + tableName + " where bid = '" + bid + "'";
			Integer delete = dynamicConnServiceV2.execDdl(dbinfo, sql);
			if (delete != 0 || delete == null) {
				result = ResultBody.createErrorResult("删除失败！");
				System.out.println(sql);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("系统错误<br>" + e.getMessage());
		}
		return result;
	}

	// 贴源库批量导入中文名
	@ResponseBody
	@RequestMapping("importTableCName")
	public ResultBody importTableCName(HttpServletRequest request) {
		ResultBody result = ResultBody.createSuccessResult("");
		try {
			String tableId = request.getParameter("tableId");
			// 文件上传的临时路径
			String tempPath = request.getParameter("tempPath");
			String real_tempPath = request.getSession().getServletContext().getRealPath(tempPath);

			ImportParams params = new ImportParams();
			params.setHeadRows(1);
			if (!(real_tempPath.toLowerCase().endsWith(".xlsx") || real_tempPath.toLowerCase().endsWith(".xls"))) {
				return ResultBody.createErrorResult("导入失败：请选择.xlsx或.xls文件导入！");
			}

			List<String> sqls = new ArrayList<String>();
			// excel表格数据 list-[{状态：是，年度：2019，。。}]
			List<Map<String, Object>> excel_list = ExcelImportUtil.importExcel(new File(real_tempPath), Map.class, params);
			for (Map<String, Object> excel_map : excel_list) {
				String columnName = excel_map.get("字段名") == null ? "" : excel_map.get("字段名").toString();
				String columnCName = excel_map.get("中文名") == null ? "" : excel_map.get("中文名").toString();
				String sql_update = "update RAW_CWTFACTOR set COLUMNCNAME='" + columnCName.trim() + "'" + " where TABLEID='" + tableId + "' and COLUMNNAME='" + columnName.trim() + "'";
				sqls.add(sql_update);
			}
			sqlMapperService.execSqls(sqls);
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("系统错误<br>" + e.getMessage());
		}
		return result;
	}

	// 贴源库导出字段名模板
	@ResponseBody
	@RequestMapping("exportTableCName")
	public ResultBody exportTableCName(HttpServletRequest request) {
		String tableCName = request.getParameter("tableCName");
		String tempPath = request.getSession().getServletContext().getRealPath("temp");
		return rawService.exportTableCName(tempPath, tableCName);
	}

	// 贴源库 -手工录入表 -导出模板
	@ResponseBody
	@RequestMapping("exportExcel_handwork")
	public ResultBody exportExcel_handwork(HttpServletRequest request) {
		String tableCName = request.getParameter("tableCName");
		String columns = request.getParameter("columns");
		String tempPath = request.getSession().getServletContext().getRealPath("temp");
		return rawService.exportExcel_handwork(tempPath, tableCName, columns);
	}

	// 手工录入表 -导出模板 带数据
	@ResponseBody
	@RequestMapping("exportExcel_handwork_haveData")
	public ResultBody exportExcel_handworkDM(HttpServletRequest request) {
		String allFieldJsonString = request.getParameter("allFieldJsonString");
		String checkFieldJsonString = request.getParameter("checkFieldJsonString");
		String tableName = request.getParameter("tableName");
		String tableCname = request.getParameter("tableCname");
		String sbtid = request.getParameter("sbtid");
		String tempPath = request.getSession().getServletContext().getRealPath("temp");
		DBInfoBean dbinfo = null;
		List<Map<String, Object>> allFieldList = null;
		List<String> checkFieldList = null;
		try {
			Map<String, Object> targetPipe = getTargetPipe(sbtid);
			dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());

			allFieldList = JSONArray.fromObject(allFieldJsonString);
			checkFieldList = JSONArray.fromObject(checkFieldJsonString);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rawService.exportExcel_handworkDM(allFieldList, checkFieldList, tableName, tableCname, dbinfo, tempPath);
	}

	// 贴源库 -手工录入表 -导入Excel
	@ResponseBody
	@RequestMapping("importExcel_handwork")
	public ResultBody importExcel_handwork(HttpServletRequest request) {
		String tableId = request.getParameter("tableId");
		String tableName = request.getParameter("tableName");
		String tempPath = request.getParameter("tempPath");
		String centreWarehouseId = request.getParameter("centreWarehouseId");
		String real_tempPath = request.getSession().getServletContext().getRealPath(tempPath);
		// 获取仓库管道
		Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId);
		DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());
		return rawService.importExcel_handwork(real_tempPath, tableId, tableName, dbinfo);
	}

	/**
	 * 批量设置录入规则
	 * 
	 * @param request
	 * @return
	 */
	@ResponseBody
	@RequestMapping("batchSetRules")
	public ResultBody batchSetRules(HttpServletRequest request) {
		try {
			String inputRules = request.getParameter("inputRules");
			String rows = request.getParameter("rows");
			List<String> sqls = new ArrayList<String>();
			List<Map<String, Object>> raw_factors = JSONArray.fromObject(rows);
			for (Map<String, Object> m : raw_factors) {
				sqls.add("update RAW_CWTFACTOR set INPUTRULES='" + inputRules + "' where guid='" + m.get("GUID") + "'");
			}
			sqlMapperService.execSqls(sqls);
			return ResultBody.createSuccessResult("保存成功！");
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("操作失败！原因：" + e.getMessage());
		}
	}

	/**
	 * 查看当前贴源库数据直采已建好的连接管道
	 * 
	 * @param cwid
	 *            贴源库id
	 * @param datapipelineId
	 *            管道id
	 * @return true : 重复 false:不重复
	 */
	@ResponseBody
	@RequestMapping("dataPipelinet_reuse")
	public Boolean dataPipelinet_reuse(String cwid, String datapipelineId, String connId) {
		String sql = "";
		if (connId != null && !"".equals(connId)) {
			sql = "select * from raw_etl_dbconnection where rawid='" + cwid + "' and datapipelineid='" + datapipelineId + "' and guid <> '" + connId + "'";
		} else {
			sql = "select * from raw_etl_dbconnection where rawid='" + cwid + "' and datapipelineid='" + datapipelineId + "'";
		}
		Map<String, Object> map = sqlMapperService.selectOne(sql);
		if (null != map) {
			return true;
		}
		return false;
	}

	@ResponseBody
	@RequestMapping("saveCollection")
	public ResultBody saveCollection(HttpServletRequest request) {
		String type = request.getParameter("type");
		String cwid = request.getParameter("cwid");
		String datapipe = request.getParameter("datapipe");
		String dbconnName = request.getParameter("dbconnName");
		if ("add".equals(type)) {
			sqlMapperService.execSql("insert into raw_etl_dbconnection (rawid,datapipelineId,connectionName) values('" + cwid + "','" + datapipe + "','" + dbconnName + "')");
		} else if ("edit".equals(type)) {
			String connId = request.getParameter("connId");
			sqlMapperService.execSql("update raw_etl_dbconnection set connectionName =  '" + dbconnName + "' where guid = '" + connId + "'");
		}
		return ResultBody.createSuccessResult("");
	}

	@ResponseBody
	@RequestMapping("saveCollections")
	public ResultBody saveCollections(HttpServletRequest request) {
		String cwid = request.getParameter("cwid");
		String rows = request.getParameter("rows").toString();
		JSONArray rowsArr = JSONArray.fromObject(rows);
		List<String> sqlList = new ArrayList<>();
		for(int i = 0;i<rowsArr.size();i++){
			JSONObject rowArr = rowsArr.getJSONObject(i);
			sqlList.add("insert into raw_etl_dbconnection (rawid,datapipelineId,connectionName) values('" + cwid + "','" +rowArr.getString("GUID") + "','" + rowArr.getString("PIPELINENAME") + "')");
		}
		sqlMapperService.execSqls2(sqlList);
		return ResultBody.createSuccessResult("");
	}

	// 获取连接信息
	@ResponseBody
	@RequestMapping("collectTableList")
	public List<Map<String, Object>> collectTableList(String cwtid) {
		String sql = "select T1.DATAPIPELINEID,T1.GUID,T1.CONNECTIONNAME,T2.DBTYPE AS CONNTYPE,T2.DBINFO AS HOSTNUMBER,to_char(T1.CREATEDATE,'yyyy-mm-dd') as CREATEDATE FROM raw_etl_dbconnection T1,BI_DATAPIPELINE T2 WHERE T1.rawid = '"
				+ cwtid + "' and T1.datapipelineId = T2.guid ";
		List<Map<String, Object>> selectList = sqlMapperService.selectList(sql);
		List<Map<String, Object>> connectList = new ArrayList<>();
		for (Map<String, Object> map : selectList) {
			JSONObject DBINFO = JSONObject.fromObject(map.get("HOSTNUMBER"));
			map.put("HOSTNUMBER", DBINFO.get("hostName"));
			connectList.add(map);
		}
		return connectList;
	}

	@ResponseBody
	@RequestMapping("removeConn")
	public ResultBody removeConn(String connId) {
		sqlMapperService.execSql2("delete from RAW_ETL_DBCONNECTION where guid = '" + connId + "'");
		sqlMapperService.execSql2("delete from raw_etl_tableconfig where CONNECTIONID ='" + connId + "'");

		return ResultBody.createSuccessResult("");
	}

	@ResponseBody
	@RequestMapping("collectInfo")
	public Map<String, Object> collectInfo(String connId, String rawid) {
		String sql = "select T1.CONNECTIONNAME,T2.DBTYPE ,T2.DBINFO,T1.DATAPIPELINEID FROM raw_etl_dbconnection T1,BI_DATAPIPELINE T2 WHERE T1.GUID = '" + connId + "' AND T1.RAWID = '" + rawid
				+ "' and T1.datapipelineId = T2.guid";
		Map<String, Object> tabMap = sqlMapperService.selectOne(sql);
		Map<String, Object> connectMap = new HashMap<>();
		connectMap.put("CONNECTIONNAME", tabMap.get("CONNECTIONNAME"));
		connectMap.put("DBTYPE", tabMap.get("DBTYPE"));
		connectMap.put("DATAPIPELINEID", tabMap.get("DATAPIPELINEID"));
		JSONObject DBINFO = JSONObject.fromObject(tabMap.get("DBINFO"));
		connectMap.put("HOSTNUMBER", DBINFO.get("hostName"));
		connectMap.put("PORTNUMBER", DBINFO.get("portNumber"));
		if (DBTypeConstant.MPPDB.equals(tabMap.get("DBTYPE"))) {
			connectMap.put("DBNAME", DBINFO.get("DBName"));
		} else {
			connectMap.put("SERVICENAME", DBINFO.get("serviceName"));
			connectMap.put("DBUID", DBINFO.get("DBUid"));
		}
		return connectMap;
	}

	// 获取贴源库表信息
	@ResponseBody
	@RequestMapping("getCdwTableList")
	public List<Map<String, Object>> getCdwTableList(String centrewarehouseid, String type, String connId) {
		String inSql = "";
		if ("01".equals(type)) {
			inSql = "select t3.tableid from raw_etl_tableconfig t3,raw_etl_dbconnection t4 where t3.rawid = '" + centrewarehouseid + "' and t3.collecttype = '" + type + "' and t4.guid = '" + connId
					+ "' and t4.guid = t3.connectionid";
		} else if ("02".equals(type)) {
			inSql = "select t3.tableid from raw_etl_tableconfig t3 where t3.rawid = '" + centrewarehouseid + "' and t3.collecttype = '" + type + "' ";
		} else if ("03".equals(type)) {
			inSql = "select t3.tableid from raw_etl_tableconfig t3,raw_etl_interface t4 where t3.rawid = '" + centrewarehouseid + "' and t3.collecttype = '" + type + "' and t4.guid = '" + connId
					+ "' and t4.guid = t3.connectionid";
		}
		String sql = "select * from raw_cwtable where CENTREWAREHOUSEID ='" + centrewarehouseid + "' and guid not in(" + inSql + ") order by tablecname";
		return sqlMapperService.selectList(sql);
	}

	// 获取贴源库表信息
	@ResponseBody
	@RequestMapping("getCdwTableTreeList")
	public List<Map<String, Object>> getCdwTableTreeList(String centrewarehouseid, String type, String connId) {
		String inSql = "";
		if ("01".equals(type)) {
			inSql = "select t3.tableid from raw_etl_tableconfig t3,raw_etl_dbconnection t4 where t3.rawid = '" + centrewarehouseid + "' and t3.collecttype = '" + type + "' and t4.guid = '" + connId
					+ "' and t4.guid = t3.connectionid";
		} else if ("02".equals(type)) {
			inSql = "select t3.tableid from raw_etl_tableconfig t3 where t3.rawid = '" + centrewarehouseid + "' and t3.collecttype = '" + type + "' ";
		} else if ("03".equals(type)) {
			inSql = "select t3.tableid from raw_etl_tableconfig t3,raw_etl_interface t4 where t3.rawid = '" + centrewarehouseid + "' and t3.collecttype = '" + type + "' and t4.guid = '" + connId
					+ "' and t4.guid = t3.connectionid";
		}
		String sql = "";
		if (!"02".equals(type)) {
			sql = "select guid id,tablename name,tablecname cname,catid pid from raw_cwtable t where CENTREWAREHOUSEID ='" + centrewarehouseid + "' and not exists(" + inSql
					+ " and t.guid = t3.tableid) " + " union all " + " select guid id,'' name,name cname, '0' pid from raw_cwtablecat c where exists( "
					+ " select distinct t1.catid from raw_cwtable t1 where CENTREWAREHOUSEID = '" + centrewarehouseid + "' and not exists ("
					+ " select t3.tableid from raw_etl_tableconfig t3, raw_etl_dbconnection t4 where t3.rawid = '" + centrewarehouseid + "' and t3.collecttype = '" + type + "'" + " and t4.guid = '"
					+ connId + "' and t4.guid = t3.connectionid  and t1.guid = t3.tableid) and c.guid = t1.catid) " + "and c.centrewarehouseid = '" + centrewarehouseid + "' order by name,cname";
		} else {
			sql = "select guid id, tablename name, tablecname cname, catid pid from raw_cwtable t where CENTREWAREHOUSEID = '" + centrewarehouseid + "' and not exists"
					+ " (select t3.tableid from raw_etl_tableconfig t3 where t3.rawid = '" + centrewarehouseid + "' and t3.collecttype = '" + type + "' and t.guid = t3.tableid)" + " union all"
					+ " select guid id, '' name, name cname, '0' pid from raw_cwtablecat c where exists" + " (select distinct t1.catid from raw_cwtable t1 where CENTREWAREHOUSEID = '"
					+ centrewarehouseid + "' and not exists" + " (select t3.tableid from raw_etl_tableconfig t3 where t3.rawid = '" + centrewarehouseid + "' and t3.collecttype = '" + type
					+ "' and t1.guid = t3.tableid)" + " and c.guid = t1.catid) and c.centrewarehouseid = '" + centrewarehouseid + "' order by name, cname";
		}
		return sqlMapperService.selectList(sql);
	}

	// 保存选中的表信息
	@ResponseBody
	@RequestMapping("saveConfigTable")
	public ResultBody saveConfigTable(HttpServletRequest request) {
		String centreWarehouseId = request.getParameter("centreWarehouseId");
		String connId = request.getParameter("connId");
		String row = request.getParameter("row");
		String type = request.getParameter("type");
		List<Map<String, Object>> rows = JSONArray.fromObject(row);
		List<String> sqls = new ArrayList<>();
		for (int i = 0; i < rows.size(); i++) {
			if ("0".equals(rows.get(i).get("PID"))) {
				continue;
			}
			sqls.add("insert into raw_etl_tableconfig(RAWID,TABLEID,COLLECTTYPE,CONNECTIONID)values('" + centreWarehouseId + "','" + rows.get(i).get("ID") + "','" + type + "','"
					+ (("".equals(connId) || null == connId) ? "" : connId) + "')");
		}
		sqlMapperService.execSqls2(sqls);
		return ResultBody.createSuccessResult("");
	}

	// 获取数据库直采保存表列表
	@ResponseBody
	@RequestMapping("getSavedTableList")
	public List<Map<String, Object>> getSavedTableList(String rawid, String connId, String type) {
		String sql = "select distinct t.tablename, t.tablecname, t2.CONNECTIONNAME as CONNNAME,t1.TABLEID,t1.GUID from raw_cwtable t,raw_etl_tableconfig t1,raw_etl_dbconnection t2 where t1.rawid = '"
				+ rawid + "'" + " and t.centrewarehouseid = t1.rawid and t1.tableid = t.guid and t1.CONNECTIONID = t2.guid and t1.COLLECTTYPE='" + type + "' and t2.guid = '" + connId
				+ "' order by t.tablecname";
		return sqlMapperService.selectList(sql);
	}

	// 获取数据库直采保存表列表
	@ResponseBody
	@RequestMapping("getSavedTableListTree")
	public List<Map<String, Object>> getSavedTableListTree(String rawid, String connId, String type) {
		String sql = "select distinct t.tablename as name, t.tablecname as cname, t2.CONNECTIONNAME as CONNNAME, t1.TABLEID,"
				+ " t1.GUID id, t.catid as pid from raw_cwtable t, raw_etl_tableconfig t1, raw_etl_dbconnection t2 where t1.rawid = '" + rawid + "'"
				+ " and t.centrewarehouseid = t1.rawid and t1.tableid = t.guid and t1.CONNECTIONID = t2.guid and t1.COLLECTTYPE = '" + type + "'" + " and t2.guid = '" + connId + "'" + " union all "
				+ " select distinct '' name3,name as cname,'' as CONNNAME,'' TABLEID, guid id, '0' pid from raw_cwtablecat where guid in(" + " select catid from raw_cwtable where guid in("
				+ " select tableid from raw_etl_tableconfig t where t.rawid = '" + rawid + "' and t.connectionid = '" + connId + "' and t.COLLECTTYPE = '" + type + "'))  and centrewarehouseid = '"
				+ rawid + "' order by cname";
		List<Map<String, Object>> sqlList = sqlMapperService.selectList(sql);
		List<Map<String, Object>> reList = new ArrayList<>();
		DataUtil.buildTreeData(reList, sqlList, "", "ID", "PID");
		return sqlList;

	}

	// 获取数据接口保存表列表
	@ResponseBody
	@RequestMapping("getInterfaceTableList")
	public List<Map<String, Object>> getInterfaceTableList(String rawid, String interfaceId, String type) {
		String sql = "select distinct t.tablename name, t.tablecname cname,t1.TABLEID,t1.GUID id,t.catid pid from raw_cwtable t,raw_etl_tableconfig t1,raw_etl_interface t2 where t1.rawid = '" + rawid
				+ "'" + " and t.centrewarehouseid = t1.rawid and t1.tableid = t.guid and t1.CONNECTIONID = t2.guid and t1.COLLECTTYPE='" + type + "' and t2.guid = '" + interfaceId + "'"
				+ " union all" + " select distinct '' name, name as cname, '' TABLEID, guid id, '0' pid from raw_cwtablecat" + " where guid in (select catid from raw_cwtable where guid in"
				+ " (select tableid from raw_etl_tableconfig t where t.rawid = '" + rawid + "' and t.connectionid = '" + interfaceId + "'" + " and t.COLLECTTYPE = '" + type
				+ "')) and centrewarehouseid = '" + rawid + "' order by cname";
		return sqlMapperService.selectList(sql);
	}

	// 获取保存表列表
	@ResponseBody
	@RequestMapping("getHandTableList")
	public List<Map<String, Object>> getHandTableList(String rawid, String type) {
		String sql = "select distinct t.tablename, t.tablecname, t1.TABLEID,t1.GUID from raw_cwtable t,raw_etl_tableconfig t1 where t1.rawid = '" + rawid + "'"
				+ " and t.centrewarehouseid = t1.rawid and t1.tableid = t.guid and t1.COLLECTTYPE='" + type + "' order by t.tablecname";
		return sqlMapperService.selectList(sql);
	}

	// 获取保存表列表
	@ResponseBody
	@RequestMapping("getHandTableTreeList")
	public List<Map<String, Object>> getHandTableTreeList(String rawid, String type) {
		String sql = "select distinct t.tablename name, t.tablecname cname, t1.TABLEID,t1.GUID id,t.catid pid from raw_cwtable t,raw_etl_tableconfig t1 where t1.rawid = '" + rawid + "'"
				+ " and t.centrewarehouseid = t1.rawid and t1.tableid = t.guid and t1.COLLECTTYPE='" + type + "' " + " union all "
				+ " select distinct '' name, name as cname, '' TABLEID, guid id, '0' pid from raw_cwtablecat where guid in" + " (select catid from raw_cwtable where guid in"
				+ " (select tableid from raw_etl_tableconfig t where t.rawid = '" + rawid + "' and t.COLLECTTYPE = '" + type + "'))" + " and centrewarehouseid = '" + rawid + "' order by cname";
		return sqlMapperService.selectList(sql);
	}

	// 获取表信息
	@ResponseBody
	@RequestMapping("getTableInfo")
	public List<Map<String, Object>> getTableInfo(String tableId) {
		String sql = "select * from raw_cwtfactor where tableid='" + tableId + "'";
		return sqlMapperService.selectList(sql);
	}

	// 保存手工录入
	@ResponseBody
	@RequestMapping("saveHandType")
	public ResultBody saveHandType(HttpServletRequest request) {
		String centreWarehouseId = request.getParameter("centreWarehouseId");
		String write = request.getParameter("write");
		String fileType = request.getParameter("fileFormat");
		Map<String, Object> map = sqlMapperService.selectOne("select * from raw_etl_handinput where RAWID = '" + centreWarehouseId + "'");
		if (map != null && map.size() > 0) {
			sqlMapperService.execSql2("update raw_etl_handinput set ISSUPPORTHAND='" + write + "',FILEFORMAT='" + ("".equals(fileType) ? "" : fileType) + "' where guid = '" + map.get("GUID") + "'");
		} else {
			sqlMapperService.execSql2("insert into raw_etl_handinput(RAWID,ISSUPPORTHAND,FILEFORMAT)values('" + centreWarehouseId + "','" + write + "','" + ("".equals(fileType) ? "" : fileType)
					+ "')");
		}
		return ResultBody.createSuccessResult("");
	}

	// 移除已选择表
	@ResponseBody
	@RequestMapping("removeTable")
	public ResultBody removeTable(String guids, String type) {
		try {
			sqlMapperService.execDdl2("delete from raw_etl_tableconfig where guid in(" + guids + ") and collecttype = '" + type + "'");
			return ResultBody.createSuccessResult("删除成功");
		} catch (SQLException e) {
			return ResultBody.createErrorResult("删除失败:" + e.getMessage());
		}
	}

	// 保存手工录入
	@ResponseBody
	@RequestMapping("getHandInfo")
	public Map<String, Object> getHandInfo(String rawid) {
		return sqlMapperService.selectOne("select * from raw_etl_handinput where rawid = '" + rawid + "'");
	}

	// 获取表的采集方式
	@ResponseBody
	@RequestMapping("getCollectInfo")
	public List<Map<String, Object>> getCollectInfo(String tableId, String rawid) {
		try {
			return mfbicommonService.getCollectInfo(tableId,rawid);
		} catch (Exception e) {
			e.printStackTrace();
			return new ArrayList<>();
		}
	}

	// 获取贴源库批量导入文件格式
	@ResponseBody
	@RequestMapping("getBathType")
	public Map<String, Object> getBathType(String rawid) {
		return sqlMapperService.selectOne("select FILEFORMAT from raw_etl_handinput where RAWID = '" + rawid + "'");
	}

	// MPP库获取主键
	@ResponseBody
	@RequestMapping("getKey")
	public List<Map<String, Object>> getKey(String rawid, String tableId) {
		return sqlMapperService.selectList("select columnname,datatype from raw_cwtfactor where tableid =  '" + tableId + "' and ispk = '1'");

	}

	// 是否支持生成代理键
	@ResponseBody
	@RequestMapping("isSurKey")
	public ResultBody isSurKey(String vid) {

		return mdmService.isSurKey(vid);
	}

	// 删除事实字段
	@ResponseBody
	@RequestMapping("/getMaxBorder")
	public String getMaxBorder(String sbtid, String tableId) {
		return sqlMapperService.selectOne("select nvl(max(ordernum),0)+1 as ordernum from raw_cwtfactor where tableId = '" + tableId + "'", String.class);
	}

	// 删除事实字段
	@ResponseBody
	@RequestMapping("/getInterface")
	public List<Map<String, Object>> getInterface(String rawid) {
		return sqlMapperService
				.selectList("select GUID,INTERNAME,INTERADDR,decode(INTERTYPE,'0','Restful','1','Web service') AS INTERTYPE,INTERHEAD,INTERPARAM,decode(RETURNTYPE,'1','JSON','2','XML') AS RETURNTYPE from raw_etl_interface where rawid = '"
						+ rawid + "'");
	}

	// 保存数据接口连接
	@ResponseBody
	@RequestMapping("/saveInterface")
	public ResultBody saveInterface(HttpServletRequest request) {
		String rawid = request.getParameter("rawid");
		String type = request.getParameter("type");
		String interData = request.getParameter("interData");
		JSONObject object = JSONObject.fromObject(interData);
		String interName = object.getString("interName");
		String interAddr = object.getString("addr");
		String interType = object.getString("interType");
		String interHead = object.getString("head");
		String remark = object.getString("remark");
		String returnType = object.getString("returnType");
		if ("add".equals(type)) {
			sqlMapperService.insert("insert into raw_etl_interface(interName,interAddr,interType,interHead,interparam,returnType,rawid) " + "values('" + interName + "','" + interAddr + "','"
					+ interType + "','" + interHead + "','" + ("".equals(remark) ? "" : remark) + "','" + returnType + "','" + rawid + "')");
		} else {
			String guid = request.getParameter("guid");
			sqlMapperService.insert("update raw_etl_interface set interName='" + interName + "',interAddr='" + interAddr + "',interType='" + interType + "',interHead='" + interHead + "',interparam='"
					+ ("".equals(remark) ? "" : remark) + "',returnType='" + returnType + "' where guid = '" + guid + "'");
		}
		return ResultBody.createSuccessResult("");
	}

	// 删除贴源库--数据接口
	@ResponseBody
	@RequestMapping("/removeInterface")
	public ResultBody removeInterface(String guids) {
		try {
			List<String> sqls = new ArrayList<>();
			sqls.add("delete from raw_etl_interface where guid in(" + guids + ") ");
			sqls.add("delete from raw_etl_tableconfig where CONNECTIONID in(" + guids + ")");
			sqlMapperService.execSqls2(sqls);
			return ResultBody.createSuccessResult("");
		} catch (Exception e) {
			return ResultBody.createErrorResult("删除失败：" + e.getMessage());
		}
	}

	// 判断贴源库表是否被中心仓库引用
	@ResponseBody
	@RequestMapping("/checkTable")
	public List<Map<String, Object>> checkTable(String rawid, String tableName) {
		return sqlMapperService.selectList("select * from bi_cw_table where SOURCECWID ='" + rawid + "' and SOURCETABLENAME='" + tableName + "'");
	}

	// 删除接口时获取数据接口相关表列表
	@ResponseBody
	@RequestMapping("getInterTabListByConnIds")
	public List<Map<String, Object>> getInterTabListByConnIds(String interfaceIds) {
		String sql = "select * from raw_etl_tableconfig  where CONNECTIONID in(" + interfaceIds + ")";
		return sqlMapperService.selectList(sql);
	}

	// 复制库查看表数据
	@ResponseBody
	@RequestMapping("getDataByTableId")
	public ResultBody getDataByTableId(String tableId) {
		String sql = "select DBTYPE,DBINFO from bi_t_dbinfo t1 where t1.centrewarehouseid = ( select t.centrewarehouseid from raw_cwtable t where t.guid = '" + tableId + "')";
		Map<String, Object> pipeline = sqlMapperService.selectOne(sql);
		DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
		boolean testConn;
		try {
			testConn = dynamicConnServiceV2.testConn(dbinfo);
			if (!testConn) {
				return ResultBody.createErrorResult("当前数据管道连接失败，请检查！");
			}
			// 获取仓库中表的列信息
			List<Map<String, Object>> factorsList = sqlMapperService.selectList("select * from raw_cwtfactor where tableid = '" + tableId + "' and INPUTRULES <> '0'");
			if (factorsList.size() == 0) {
				return ResultBody.createErrorResult("数据检索错误！");
			}
			String cols_ = "";
			for (Map<String, Object> cols : factorsList) {
				String DATA_TYPE = cols.get("DATATYPE").toString();
				String COLUMNNAME = cols.get("COLUMNNAME").toString();
				if (DATA_TYPE.equals("日期型")) {
					if (DBTypeConstant.MPPDB.equals(dbinfo.DBTYPE)) {
						cols_ += "from_unixtime(unix_timestamp(" + COLUMNNAME + "),'yyyy-MM-dd hh:mm:ss') as " + COLUMNNAME + ",";
					} else {
						cols_ += "to_char(" + COLUMNNAME + ",'yyyy-mm-dd hh:mi:ss') as " + COLUMNNAME + ",";
					}
				} else {
					cols_ += COLUMNNAME + ",";
				}
			}

			cols_ = cols_.substring(0, cols_.length() - 1);
			String tableName = sqlMapperService.selectOne("select t.TABLENAME from raw_cwtable t where t.guid = '" + tableId + "'", String.class);
			String sqlList = "select " + cols_ + " from " + tableName + " where rownum <= 50";

			// 获取仓库中表数据
			List<Map<String, Object>> dataList = dynamicConnServiceV2.selectList(dbinfo, sqlList);

			Map<String, Object> resMap = new HashMap<String, Object>();
			resMap.put("factorsList", factorsList);
			resMap.put("dataList", dataList);
			resMap.put("DBTYPE", pipeline.get("DBTYPE").toString());
			return ResultBody.createSuccessResult(resMap);
		} catch (Exception e) {
			return ResultBody.createErrorResult("数据获取失败：" + e.getMessage());
		}

	}

	// 根据贴源库id获取审核规则
	@ResponseBody
	@RequestMapping("getPolicy")
	public List<Map<String, Object>> getPolicy(String tableId, String rawguid) {
		// String sql =
		// "select t.guid,t.policyname,t.policycode,t1.cname,t.auditlevel,t.auditlevel as auditlevel_,t.policytype,t.status,b.cname as POLICYTYPENAME from BI_AUDITPOLICY t,BI_AUDITPOLICY_CATE t1,bi_code b"
		// +
		// " where t1.rawguid = '"+rawguid+"' and t.policycateid = t1.id  and t.busitableid = '"+tableId+"' and t.policytype = b.coden and b.basename='AUDITPOLICY'";
		String sql = "select t.guid, t.policyname, t.policycode,t.PROCEDURENAME, t1.cname, t.auditlevel, t.auditlevel as auditlevel_, t.policytype, t.status, b.cname as POLICYTYPENAME,sum(l.policydata) count ,l.policyid "
				+ " from BI_AUDITPOLICY t, BI_AUDITPOLICY_CATE t1, bi_code b,RAW_POLICY_PROGR_LOG l "
				+ " where t1.rawguid = '"
				+ rawguid
				+ "' and t.policycateid = t1.id and t.busitableid = '"
				+ tableId
				+ "' "
				+ " and t.policytype = b.coden and b.basename = 'AUDITPOLICY' and l.policyid = t.guid "
				+ "group by t.guid,t.policyname,t.policycode,t1.cname,t.auditlevel,t.policytype,t.status,b.cname,l.policyid,t.PROCEDURENAME";
		return sqlMapperService.selectList(sql);
	}

	/**
	 * 获取业务表列信息
	 * 
	 * @param sourcePipelineId
	 * @param tableName
	 * @return
	 */
	@RequestMapping("/getBusiTableCols")
	public @ResponseBody
	ResultBody getBusiTableCols(String sourcePipelineId, String tableName) {
		try {
			// 数据管道
			Map<String, Object> pipeline = this.getPipeline(sourcePipelineId);
			if (pipeline == null) {
				return ResultBody.createErrorResult("数据管道信息获取失败，请检查！");
			}

			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
			boolean testConn = dynamicConnServiceV2.testConn(dbinfo);
			if (!testConn) {
				return ResultBody.createErrorResult("当前数据管道连接失败，请检查！");
			}

			// 获取仓库中表的列信息
			List<Map<String, Object>> factorsList = this.getCWFactorsList(dbinfo, tableName, null);
			return ResultBody.createSuccessResult(factorsList);
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("系统错误！原因：" + e.getMessage());
		}
	}

	/**
	 * 验证表数据过滤条件（多表合并创建第三步）
	 */
	@RequestMapping("/validationFilterConditions")
	public @ResponseBody
	ResultBody validationFilterConditions(String param, String dataPipelineId, String type, String tableMessage, String tableLinks) {
		// List<YwkTableRows> rList = new ArrayList<>();
		try {
			// 来源数据管道
			Map<String, Object> pipeline = this.getPipeline(dataPipelineId);
			if (pipeline == null) {
				return ResultBody.createErrorResult("来源数据管道信息获取失败，请检查！");
			}

			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
			boolean testConn = dynamicConnServiceV2.testConn(dbinfo);
			if (!testConn) {
				return ResultBody.createErrorResult("来源数据管道连接失败，请检查！");
			}
			StringBuffer sbf = new StringBuffer();
			if ("3".equals(type)) {
				List<Map<String, Object>> busiTableList = JSONArray.fromObject(param);
				List<Map<String, Object>> tableLinksList = JSONArray.fromObject(tableLinks);
				sbf.append("select count(1) from ");
				// 有几个验几个
				for (Map<String, Object> row : busiTableList) {
					String tableName = row.get("TABLE_NAME").toString();
					String ALISA = row.get("ALISA").toString();
					sbf.append("(select count(1) from " + tableName + " " + ALISA);
					sbf.append(" where ");
					if (row.get("TABLE_FILTERCONDI") != null && !"".equals(row.get("TABLE_FILTERCONDI"))) {
						sbf.append(row.get("TABLE_FILTERCONDI") + " and ");
					}
					sbf.append(" 1 = 1 ");
					sbf.append(") A" + ALISA + ",");
				}
				if (tableLinksList != null && tableLinksList.size() > 0) {
					String primaryTableName = tableLinksList.get(0).get("LEFT_TABLE").toString();
					String primaryTableALISA = tableLinksList.get(0).get("LEFT_TABLE_ALISA").toString();
					sbf.append("(select count(1) from " + primaryTableName + " " + primaryTableALISA);
					for (Map<String, Object> tableLink : tableLinksList) {
						sbf.append(" " + tableLink.get("JOINTYPE") + " " + tableLink.get("RIGHT_TABLE") + " " + tableLink.get("RIGHT_TABLE_ALISA") + " on " + tableLink.get("LEFT_TABLE_ALISA") + "."
								+ tableLink.get("LEFTTABLE_FIELD") + " = " + tableLink.get("RIGHT_TABLE_ALISA") + "." + tableLink.get("RIGHTTABLE_FIELD"));

					}
					sbf.append(") AB" + ",");
				}
				sbf = sbf.deleteCharAt(sbf.length() - 1);
			} else if ("5".equals(type)) {
				sbf.append(" select count(1) from ");
				List<Map<String, Object>> tableList = JSONArray.fromObject(tableMessage);
				for (Map<String, Object> table : tableList) {
					sbf.append(table.get("TABLE_NAME") + " " + table.get("ALISA") + ",");
				}
				sbf = sbf.deleteCharAt(sbf.length() - 1);
				if (param != null && !"".equals(param)) {
					sbf.append(" where " + param);
				}
			}
			dynamicConnServiceV2.execSql2(dbinfo, sbf.toString());

			return ResultBody.createSuccessResult("");
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("过滤条件或表关联关系有错误！原因：" + e.getMessage());
		}

	}

	/**
	 * 从业务库建表（多来源表合并）
	 * 
	 * @param catId
	 *            贴源库分类id
	 * @param centreWarehouseId
	 *            当前贴源库ID
	 * @param dataPipelineId
	 *            业务库管道ID
	 * @param selBusiTables
	 *            已选来源表
	 * @param tableLinksData
	 *            来源表关联关系
	 * @param busiTableColsData
	 *            已选来源表字段
	 * @param multiTabCondiCentent
	 *            其他过滤条件
	 * @param tableName
	 *            目标表名称
	 * @param tableCName
	 *            目标表中文名称
	 * @param annotation
	 *            目标表备注
	 * @param isRunTrans
	 *            是否执行抽取数据
	 * @param extractType
	 *            抽取方式 0：全量 1：增量
	 * @return
	 */
	@RequestMapping("/createTableMultiSource")
	@ResponseBody
	public ResultBody createTableMultiSource(HttpServletRequest request, String catId, String centreWarehouseId, String dataPipelineId, String selBusiTables, String tableLinksData,
			String busiTableColsData, String multiTabCondiCentent, String tableName, String tableCName, String annotation, String isRunTrans, String extractType) {
		boolean isErrorTag = false;// 判断程序哪部分出错
		DBInfoBean dbinfo_tData = null;
		try {
			final String NL = " ";
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
			String sql = "SELECT t.cname rawNAME,t.datacategoryid from Raw_Centrewarehouse t WHERE T.GUID='" + centreWarehouseId + "'";
			Map<String, Object> rawObj = sqlMapperService.selectOne(sql);

			// 来源数据管道
			Map<String, Object> pipeline = this.getPipeline(dataPipelineId);
			if (pipeline == null) {
				return ResultBody.createErrorResult("来源数据管道信息获取失败，请检查！");
			}
			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
			boolean testConn = dynamicConnServiceV2.testConn(dbinfo);
			if (!testConn) {
				return ResultBody.createErrorResult("来源数据管道连接失败，请检查！");
			}

			Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId); // 仓库目标管道
			if (targetPipe == null) {
				ResultBody.createErrorResult("目标数据管道信息获取失败，请检查！");
			}

			DBInfoBean dbinfo_t = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());
			boolean testConn_t = dynamicConnServiceV2.testConn(dbinfo_t);
			if (!testConn_t) {
				return ResultBody.createErrorResult("目标数据管道连接失败，请检查！");
			}

			// 来源表
			StringBuffer sbf_tabs = new StringBuffer();
			// 表关联关系
			StringBuffer sbf_links = new StringBuffer();
			// 来源表字段
			StringBuffer sbf_cols = new StringBuffer();
			// 来源表表内过滤条件
			StringBuffer sbf_tableCondi = new StringBuffer();

			// 来源表信息
			List<Map<String, Object>> busiTableList = JSONArray.fromObject(selBusiTables);

			// 已选来源表字段信息
			List<Map<String, Object>> busiTableColsList = JSONArray.fromObject(busiTableColsData);

			// 表关联关系
			List<Map<String, Object>> tableLinksList = JSONArray.fromObject(tableLinksData);

			// 多个来源表与目标表的关系集合
			List<Map<String, Object>> rawTableInfoList = new ArrayList<>();

			StringBuffer sTableName = new StringBuffer();
			StringBuffer sTableCName = new StringBuffer();
			for (Map<String, Object> tabMap : busiTableList) {
				String tableName_s = tabMap.get("TABLE_NAME").toString(); // 来源表名
				String tableCName_s = tabMap.get("TABLE_CNAME").toString(); // 来源表中文名
				String tableNameAlisa = tabMap.get("ALISA").toString(); // 来源表名别名
				sbf_tabs.append(NL + tableName_s + NL + tableNameAlisa + ",");

				if (tabMap.get("TABLE_FILTERCONDI") != null && !"".equals(tabMap.get("TABLE_FILTERCONDI"))) {
					sbf_tableCondi.append(" and " + tabMap.get("TABLE_FILTERCONDI"));
				}

				Map<String, Object> rawTableInfo = new HashMap<String, Object>();
				rawTableInfo.put("TABLE_NAME", tableName_s);
				rawTableInfo.put("TABLE_CNAME", tableCName_s);
				rawTableInfo.put("tableName", tableName);
				rawTableInfo.put("tableCName", tableCName);
				rawTableInfoList.add(rawTableInfo);

				sTableName.append(tableName_s + ",");
				sTableCName.append(tableCName_s + ",");
			}
			sbf_tabs = sbf_tabs.deleteCharAt(sbf_tabs.length() - 1);
			sTableName = sTableName.deleteCharAt(sTableName.length() - 1);
			sTableCName = sTableCName.deleteCharAt(sTableCName.length() - 1);

			// 拼表关联关系sql;
			if (tableLinksList != null && tableLinksList.size() > 0) {
				String primaryTableName = tableLinksList.get(0).get("LEFT_TABLE").toString();
				String primaryTableALISA = tableLinksList.get(0).get("LEFT_TABLE_ALISA").toString();
				sbf_links.append(primaryTableName + " " + primaryTableALISA);
				for (Map<String, Object> tableLink : tableLinksList) {
					sbf_links.append(" " + tableLink.get("JOINTYPE") + " " + tableLink.get("RIGHT_TABLE") + " " + tableLink.get("RIGHT_TABLE_ALISA") + " on " + tableLink.get("LEFT_TABLE_ALISA") + "."
							+ tableLink.get("LEFTTABLE_FIELD") + " = " + tableLink.get("RIGHT_TABLE_ALISA") + "." + tableLink.get("RIGHTTABLE_FIELD"));
				}
			}

			if (multiTabCondiCentent != null && !"".equals(multiTabCondiCentent)) {
				sbf_tableCondi.append(" and " + multiTabCondiCentent);
			}

			List<TableInputOutField> fields = new ArrayList<TableInputOutField>();
			for (Map<String, Object> busiTableCol : busiTableColsList) {
				String COLUMN_NAME = busiTableCol.get("COLUMN_NAME").toString(); // 来源列名称
				String COLUMNNAME_NEW = busiTableCol.get("COLUMNNAME_NEW").toString(); // 目标列名称

				// 公式类型
				String FORMULATYPE = busiTableCol.get("FORMULATYPE").toString();
				if (FORMULATYPE == null || FORMULATYPE.equals("")) {
					sbf_cols.append(busiTableCol.get("ALISA") + "." + COLUMN_NAME);
				} else {
					String FORMULATEXT = busiTableCol.get("FORMULATEXT").toString();
					sbf_cols.append("(" + FORMULATEXT + ")");
				}
				sbf_cols.append(" as " + COLUMNNAME_NEW + ",");
				String factorType = busiTableCol.get("DATA_TYPE").toString();
				String _type = TableInputOutField.TYPE_STRING;
				if ("整型".equals(factorType) || "浮点型".equals(factorType)) {
					_type = TableInputOutField.TYPE_NUMBER;
				} else if ("日期型".equals(factorType)) {
					_type = TableInputOutField.TYPE_DATE;
				}

				TableInputOutField tableInputOutField = new TableInputOutField(COLUMN_NAME, COLUMNNAME_NEW, _type);
				if (busiTableCol.get("ZLGXYJ") != null && !"".equals(busiTableCol.get("ZLGXYJ")) && "1".equals(busiTableCol.get("ZLGXYJ"))) {
					tableInputOutField.setIskey(true);
				}
				fields.add(tableInputOutField);
				busiTableCol.put("COLUMN_NAME", busiTableCol.get("COLUMNNAME_NEW"));
			}
			sbf_cols = sbf_cols.deleteCharAt(sbf_cols.length() - 1);

			if (tableLinksList != null && tableLinksList.size() > 0) {
				sbf_tabs = sbf_links;
			}

			// 表输入SQL
			String selectSql = "select " + sbf_cols + " from " + sbf_tabs + " where 1 = 1 " + sbf_tableCondi.toString();
			JSONObject params = new JSONObject();
			params.put("dataSource", EtlConstant.SOURCE_TYK);
			params.put("description", "从业务库创建贴源库表（基于多来源表合并创建功能）");
			dbinfo.DBNAMECN = pipeline.get("PIPELINENAME").toString();
			dbinfo_t.DBNAMECN = targetPipe.get("PIPELINENAME_REAL").toString();
			dbinfo_tData = dbinfo_t;

			List<Map<String, Object>> cwFactors = this.getCWFactors(null, busiTableColsList);

			// 创建仓库物理表
			ResultBody createCDWTable = this.createCDWTable(dbinfo_t, tableName, tableCName, cwFactors, null);
			if (createCDWTable.isError) {
				return createCDWTable;
			}

			Map<String, Object> rawTabledef = new HashMap<String, Object>();
			rawTabledef.put("TABLE_NAME", sTableName);
			rawTabledef.put("TABLE_CNAME", sTableCName);
			rawTabledef.put("tableName", tableName);
			rawTabledef.put("tableCName", tableCName);
			// 保存贴源库表定义
			boolean saveCWTableDefin = this.saveCWTableDefin(centreWarehouseId, rawTabledef, cwFactors, "03", targetPipe.get("GUID").toString(), user.UserName, catId, user.MID, dataPipelineId);
			if (!saveCWTableDefin) {
				// 贴源库定义保存失败，删除仓库表
				this.dropCDWTable(dbinfo_t, tableName);
				return ResultBody.createErrorResult("贴源库定义保存失败，请检查贴源库表定义！");
			}
			isErrorTag = true;

			String tableId = sqlMapperService.selectOne("select t.GUID from RAW_CWTABLE t where t.TABLENAME='" + tableName + "' and t.CENTREWAREHOUSEID='" + centreWarehouseId + "'", String.class);

			// 贴源库所属目录
			String rawDirectories = this.getRawDirectories(rawObj.get("DATACATEGORYID").toString());
			// 创建转换
			TransMetaConfig transMetaConfig = rawService.getTransMetaConfig(extractType, dbinfo, dbinfo_t, selectSql, tableName, fields, tableCName, rawDirectories + "/" + rawObj.get("RAWNAME"), params);
			ResultBody result_createTrans = packageService.createTrans(user.optAdmdivCode, transMetaConfig);
			if (result_createTrans.isError) {
				this.dropCDWTable(dbinfo_t, tableName);// 删物理表
				this.delCDWTabFactor(tableId);// 删逻辑表
				return result_createTrans;
			}
			Map<String, Object> _map = (Map<String, Object>) result_createTrans.result;
			String transId = _map.get("transId").toString();// 转换id

			// 多个来源表(目标表对血缘关系 一对多)
			for (Map<String, Object> rawTableInfo : rawTableInfoList) {
				// 保存血缘关系数据
				mfbicommonService.saveDLS_RelRaw_RawToRaw(dataPipelineId, centreWarehouseId, rawTableInfo, dbinfo, dbinfo_t);
			}
			// 贴源库表定义存储转换id
			String sql_ = "update RAW_CWTABLE set TRANSID=" + transId + " where CENTREWAREHOUSEID='" + centreWarehouseId + "' and upper(TABLENAME)='" + tableName.toUpperCase() + "'";
			sqlMapperService.execSql2(sql_);

			// 为新建的表默认选择采集方式(数据库直采)
			setDefaultCollection(centreWarehouseId, dataPipelineId, tableId);

			// 保存从业务库批量建表相关信息 以及创建转换之后向raw_source_infomain中更新转换id和转换名称
			saveRawSourceInfo(centreWarehouseId, dataPipelineId, busiTableList, busiTableColsList, tableLinksList, tableName, transId, tableCName);
			// 保存转换信息到转换配置表
			mfbicommonService.create_eltransinfo(user.optAdmdivCode, Integer.parseInt(transId), centreWarehouseId, tableId, "01", "1", user.UserName, extractType);

			if ("1".equals(isRunTrans)) {
				packageService.runTrans2(user.admdivCode, Integer.valueOf(transId));
			}

			return ResultBody.createSuccessResult("成功创建表：</br>" + tableCName + "【" + tableName + "】");
		} catch (Exception e) {
			if (isErrorTag) {
				this.dropCDWTable(dbinfo_tData, tableName);
			}
			e.printStackTrace();
			return ResultBody.createErrorResult("系统错误！原因：" + e.getMessage());
		}
	}

	/**
	 * 新增转换
	 * 
	 * @param request
	 * @param catId
	 *            贴源库分类id
	 * @param centreWarehouseId
	 *            仓库id
	 * @param dataPipelineId
	 *            管道id
	 * @param selBusiTables
	 *            表数据
	 * @param tableLinksData
	 *            表关联管理数据
	 * @param lastBusiTableCols
	 *            目标字段对应来源字段信息
	 * @param multiTabCondiCentent
	 *            表过滤条件
	 * @param tableName
	 *            物理表表
	 * @param tableCName
	 *            表中文名
	 * @param transFormName
	 *            转换名称
	 * @param extractType
	 *            抽取方式 0：全量 1：增量
	 * @return
	 */
	@RequestMapping("/createTransformation")
	@ResponseBody
	public ResultBody createTransformation(HttpServletRequest request, String catId, String centreWarehouseId, String dataPipelineId, String selBusiTables, String tableLinksData,
			String lastBusiTableCols, String multiTabCondiCentent, String tableName, String tableCName, String transFormName, String extractType) {
		try {
			final String NL = " ";
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
			String sql = "SELECT t.cname rawNAME,t.datacategoryid from Raw_Centrewarehouse t WHERE T.GUID='" + centreWarehouseId + "'";

			Map<String, Object> rawObj = sqlMapperService.selectOne(sql);
			// 来源数据管道
			Map<String, Object> pipeline = this.getPipeline(dataPipelineId);
			if (pipeline == null) {
				return ResultBody.createErrorResult("来源数据管道信息获取失败，请检查！");
			}

			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(pipeline.get("DBTYPE").toString(), pipeline.get("DBINFO").toString());
			boolean testConn = dynamicConnServiceV2.testConn(dbinfo);
			if (!testConn) {
				return ResultBody.createErrorResult("来源数据管道连接失败，请检查！");
			}

			Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId); // 仓库目标管道
			if (targetPipe == null) {
				ResultBody.createErrorResult("目标数据管道信息获取失败，请检查！");
			}

			DBInfoBean dbinfo_t = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());
			boolean testConn_t = dynamicConnServiceV2.testConn(dbinfo_t);
			if (!testConn_t) {
				return ResultBody.createErrorResult("目标数据管道连接失败，请检查！");
			}

			// 来源表
			StringBuffer sbf_tabs = new StringBuffer();
			// 表关联关系
			StringBuffer sbf_links = new StringBuffer();
			// 来源表字段
			StringBuffer sbf_cols = new StringBuffer();
			// 来源表表内过滤条件
			StringBuffer sbf_tableCondi = new StringBuffer();

			// 来源表信息
			List<Map<String, Object>> busiTableList = JSONArray.fromObject(selBusiTables);

			// 已选来源表字段信息
			List<Map<String, Object>> busiTableColsList = JSONArray.fromObject(lastBusiTableCols);

			// 表关联关系
			List<Map<String, Object>> tableLinksList = JSONArray.fromObject(tableLinksData);
			// 多个来源表与目标表的关系集合
			List<Map<String, Object>> rawTableInfoList = new ArrayList<>();

			for (Map<String, Object> tabMap : busiTableList) {
				String tableName_s = tabMap.get("TABLE_NAME").toString(); // 来源表名
				String tableCName_s = tabMap.get("TABLE_CNAME").toString(); // 来源表名
				String tableNameAlisa = tabMap.get("ALISA").toString(); // 来源表名别名
				sbf_tabs.append(NL + tableName_s + NL + tableNameAlisa + ",");

				if (tabMap.get("TABLE_FILTERCONDI") != null && !"".equals(tabMap.get("TABLE_FILTERCONDI"))) {
					sbf_tableCondi.append(" and " + tabMap.get("TABLE_FILTERCONDI"));
				}

				Map<String, Object> rawTableInfo = new HashMap<String, Object>();
				rawTableInfo.put("TABLE_NAME", tableName_s);
				rawTableInfo.put("TABLE_CNAME", tableCName_s);
				rawTableInfo.put("tableName", tableName);
				rawTableInfo.put("tableCName", tableCName);
				rawTableInfoList.add(rawTableInfo);
			}
			sbf_tabs = sbf_tabs.deleteCharAt(sbf_tabs.length() - 1);

			// 拼表关联关系sql
			if (tableLinksList != null && tableLinksList.size() > 0) {
				String primaryTableName = tableLinksList.get(0).get("LEFT_TABLE").toString();
				String primaryTableALISA = tableLinksList.get(0).get("LEFT_TABLE_ALISA").toString();
				sbf_links.append(primaryTableName + " " + primaryTableALISA);
				for (Map<String, Object> tableLink : tableLinksList) {
					sbf_links.append(" " + tableLink.get("JOINTYPE") + " " + tableLink.get("RIGHT_TABLE") + " " + tableLink.get("RIGHT_TABLE_ALISA") + " on " + tableLink.get("LEFT_TABLE_ALISA") + "."
							+ tableLink.get("LEFTTABLE_FIELD") + " = " + tableLink.get("RIGHT_TABLE_ALISA") + "." + tableLink.get("RIGHTTABLE_FIELD"));
				}
			}

			if (multiTabCondiCentent != null && !"".equals(multiTabCondiCentent)) {
				sbf_tableCondi.append(" and " + multiTabCondiCentent);
			}

			List<TableInputOutField> fields = new ArrayList<TableInputOutField>();

			// 拼接sbf_cols
			if (busiTableColsList.size() != 0) {
				for (Map<String, Object> busiTableCol : busiTableColsList) {
					sbf_cols.append("(" + busiTableCol.get("SOURCECOLUMNNAME") + ")" + " as " + busiTableCol.get("COLUMNNAME_NEW") + ",");

					String COLUMNNAME_NEW = busiTableCol.get("COLUMNNAME_NEW").toString(); // 目标列名称
					String factorType = busiTableCol.get("DATA_TYPE").toString();
					String _type = TableInputOutField.TYPE_STRING;
					if ("整型".equals(factorType) || "浮点型".equals(factorType)) {
						_type = TableInputOutField.TYPE_NUMBER;
					} else if ("日期型".equals(factorType)) {
						_type = TableInputOutField.TYPE_DATE;
					}
					TableInputOutField tableInputOutField = new TableInputOutField(COLUMNNAME_NEW, COLUMNNAME_NEW, _type);
					if (busiTableCol.get("ZLGXYJ") != null && !"".equals(busiTableCol.get("ZLGXYJ")) && "1".equals(busiTableCol.get("ZLGXYJ"))) {
						tableInputOutField.setIskey(true);
					}
					fields.add(tableInputOutField);
				}
				sbf_cols = sbf_cols.deleteCharAt(sbf_cols.length() - 1);
			} else {// 未选任何字段的转换
				sbf_cols.append("*");
			}

			if (tableLinksList != null && tableLinksList.size() > 0) {
				sbf_tabs = sbf_links;
			}

			// 表输入SQL
			String selectSql = "select " + sbf_cols + " from " + sbf_tabs + " where 1 = 1 " + sbf_tableCondi.toString();
			JSONObject params = new JSONObject();
			params.put("dataSource", EtlConstant.SOURCE_TYK);
			params.put("description", "从业务库创建贴源库表（基于多来源表合并创建功能）");
			dbinfo.DBNAMECN = pipeline.get("PIPELINENAME").toString();
			dbinfo_t.DBNAMECN = targetPipe.get("PIPELINENAME_REAL").toString();

			List<Map<String, Object>> busiTableColsListNew = JSONArray.fromObject(lastBusiTableCols);
			for (Map<String, Object> busiTableCol : busiTableColsListNew) {
				if ("01".equals(busiTableCol.get("TYPE_"))) {
					busiTableCol.put("COLUMN_NAME", busiTableCol.get("SOURCECOLUMNNAME").toString().split("\\.")[1].toString());
					String alisa = busiTableCol.get("SOURCECOLUMNNAME").toString().split("\\.")[0].toString();
					for (Map<String, Object> tableLMap : busiTableList) {
						if (tableLMap.get("ALISA").equals(alisa)) {
							busiTableCol.put("TABLE_NAME", tableLMap.get("TABLE_NAME"));
						}
					}
				} else {
					busiTableCol.put("COLUMN_NAME", "");
					busiTableCol.put("TABLE_NAME", "");
				}
			}


			// 贴源库所属目录
			String rawDirectories = this.getRawDirectories(rawObj.get("DATACATEGORYID").toString());
			// 创建转换
			TransMetaConfig transMetaConfig = rawService.getTransMetaConfig(extractType, dbinfo, dbinfo_t, selectSql, tableName, fields, transFormName, rawDirectories + "/" + rawObj.get("RAWNAME"), params);
			ResultBody result_createTrans = packageService.createTrans(user.optAdmdivCode, transMetaConfig);
			if (result_createTrans.isError) {
				return ResultBody.createErrorResult("系统错误！原因：" + result_createTrans.errMsg);
			}
			Map<String, Object> _map = (Map<String, Object>) result_createTrans.result;
			String transId = _map.get("transId").toString();// 转换id

			String tableId = sqlMapperService.selectOne("select t.GUID from RAW_CWTABLE t where t.TABLENAME='" + tableName + "' and t.CENTREWAREHOUSEID='" + centreWarehouseId + "'", String.class);


			// 多个来源表(目标表对血缘关系 一对多)
			for (Map<String, Object> rawTableInfo : rawTableInfoList) {
				// 保存血缘关系数据
				mfbicommonService.saveDLS_RelRaw_RawToRaw(dataPipelineId, centreWarehouseId, rawTableInfo, dbinfo, dbinfo_t);
			}

			// 未选任何字段的转换
			if (busiTableColsList.size() != 0) {
				// 保存从业务库批量建表相关信息 以及创建转换之后向raw_source_infomain中更新转换id和转换名称
				saveRawSourceInfo(centreWarehouseId, dataPipelineId, busiTableList, busiTableColsListNew, tableLinksList, tableName, transId, tableCName);
			}
			// 保存转换信息到转换配置表
			mfbicommonService.create_eltransinfo(user.optAdmdivCode, Integer.parseInt(transId), centreWarehouseId, tableId, "01", "1", user.UserName, extractType);
			HashMap<String, Object> map = new HashMap<>();
			map.put("successMessage", "成功创建转换");
			map.put("transId", transId);
			return ResultBody.createSuccessResult(map);
		} catch (Exception e) {
			return ResultBody.createErrorResult("系统错误！原因：" + e.getMessage());
		}
	}

	/**
	 * 保存从业务库批量建表相关信息
	 * 
	 * @param centreWarehouseId
	 *            仓库id
	 * @param dataPipelineId
	 *            管道id
	 * @param busiTableList
	 *            表数据
	 * @param busiTableColsList
	 *            表字段数据
	 * @param tableLinksList
	 *            表关联关系数据
	 * @param tableName
	 *            表名
	 * @param transId
	 *            表id
	 * @param tableCName
	 *            表中文名
	 */
	public void saveRawSourceInfo(String centreWarehouseId, String dataPipelineId, List<Map<String, Object>> busiTableList, List<Map<String, Object>> busiTableColsList,
			List<Map<String, Object>> tableLinksList, String tableName, String transId, String tableCName) {
		// 保存从业务库批量建表相关信息
		String guid_main = rawService.saveTableMultiSourceinfo(centreWarehouseId, dataPipelineId, busiTableList, busiTableColsList, tableLinksList, tableName);

		// 更新转换信息到raw_source_infomain中
		String sql_1 = "update RAW_SOURCE_INFOMAIN set TRANSID = '" + transId + "',TRANSNAME = '" + tableCName + "' where GUID = '" + guid_main + "'";
		sqlMapperService.execSql2(sql_1);
	}

	/**
	 * 根据表id查询转换配置信息
	 * 
	 * @param tableId
	 *            表id
	 * @return
	 */
	@RequestMapping("/getRawEtlTableconfig")
	@ResponseBody
	public List<Map<String, Object>> getRawEtlTableconfig(String tableId) {
		String sqlString = "select * from raw_etl_tableconfig where TABLEID = '" + tableId + "'";
		List<Map<String, Object>> rawEtlTableconfig = sqlMapperService.selectList(sqlString);
		return rawEtlTableconfig;
	}

	/**
	 * 获取该表所关联的管道信息
	 * 
	 * @param tableId
	 *            表id
	 * @return
	 */
	@RequestMapping("/collectgetPipelineInfo")
	@ResponseBody
	public List<Map<String, Object>> collectgetPipelineInfo(String tableId) {
		String sqlString = "select * from bi_datapipeline where guid in (" + "select datapipelineid from raw_etl_dbconnection where guid in ("
				+ "select connectionid from raw_etl_tableconfig where tableid = '" + tableId + "'))";
		List<Map<String, Object>> pipelineInfoList = sqlMapperService.selectList(sqlString);
		return pipelineInfoList;
	}

	/**
	 * 获取该表所关联的管道信息
	 * 
	 * @param sbtid
	 *            表id
	 * @return
	 */
	@RequestMapping("/getTypeBySbtid")
	@ResponseBody
	public Map<String,Object> getTypeBySbtid(String sbtid) {
		String sqlString = "select basetype from bi_subject where sbtid = '" + sbtid + "'";
		return  sqlMapperService.selectOne(sqlString);
	}
    /**
     * 获取该表采集方式中设置的所有管道id
     *
     * @param tableId 表id
     * @return
     */
    @RequestMapping("/getThisTableCollectmannerResult")
    @ResponseBody
    public List<String> getThisTableCollectmannerResult(String tableId) {
        String sqlString = "select CONNECTIONID from RAW_ETL_TABLECONFIG where TABLEID = '" + tableId + "'";
        List<String> dataPipelineIds = sqlMapperService.selectList(sqlString, String.class);
        return dataPipelineIds;
    }
	// 保存选中的表信息
	@ResponseBody
	@RequestMapping("saveConfigTableInDataDetail")
	public ResultBody saveConfigTableInDataDetail(HttpServletRequest request) {
		try {
			String rawId = request.getParameter("RAWID");
			String tableId = request.getParameter("TABLEID");
			String collectMannerRows = request.getParameter("collectMannerRows");
			String collectMannerRowsNeedDelete = request.getParameter("collectMannerRowsNeedDelete");
			List<Map<String, Object>> collectMannerDataList = JSONArray.fromObject(collectMannerRows);
			List<String> collectMannerRowsNeedDeleteDataList = JSONArray.fromObject(collectMannerRowsNeedDelete);
			List<String> sqls = new ArrayList<>();
			for (Map<String, Object> collectMannerDataMap : collectMannerDataList) {
				collectMannerDataMap.put("CONNTYPE", collectMannerDataMap.get("CONNTYPE") == "ORACLE" ? "01": "02");
				sqls.add("insert into raw_etl_tableconfig(RAWID,TABLEID,COLLECTTYPE,CONNECTIONID)values('" + rawId + "','" + tableId + "','01','"
						+ collectMannerDataMap.get("GUID") + "')");
			}
			for (int i = 0; i < collectMannerRowsNeedDeleteDataList.size(); i++) {
				if( !JSONNull.getInstance().equals(collectMannerRowsNeedDeleteDataList.get(i)) && collectMannerRowsNeedDeleteDataList.get(i) != null && !"null".equals(collectMannerRowsNeedDeleteDataList.get(i))) {
					sqls.add("delete from RAW_ETL_TABLECONFIG where TABLEID = '"+tableId+"' and CONNECTIONID = '"+ collectMannerRowsNeedDeleteDataList.get(i) +"'");
				}
			}
			sqlMapperService.execSqls2(sqls);
			return ResultBody.createSuccessResult("");
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult(e.toString());
		}
	}

	// 贴源库批量导入贴源库数据
	@ResponseBody
	@RequestMapping("importTable")
	public ResultBody importTable(HttpServletRequest request) {
		ResultBody result = ResultBody.createSuccessResult("");
		try {
			String tableName = request.getParameter("tableName");
			String tableId = request.getParameter("tableId");
			// 文件上传的临时路径
			String tempPath = request.getParameter("tempPath");
			String real_tempPath = request.getSession().getServletContext().getRealPath(tempPath);

			ImportParams params = new ImportParams();
			params.setHeadRows(1);
			if (!(real_tempPath.toLowerCase().endsWith(".xlsx") || real_tempPath.toLowerCase().endsWith(".xls"))) {
				return ResultBody.createErrorResult("导入失败：请选择.xlsx或.xls文件导入！");
			}
			List<String> sqls = new ArrayList<String>();
			// excel表格数据 list-[{状态：是，年度：2019，。。}]
			List<Map<String, Object>> excel_list = ExcelImportUtil.importExcel(new File(real_tempPath), Map.class, params);
			//根据表id获取列信息
			List<Map<String,Object>> factorList = sqlMapperService.selectList("select columnname,columncname from raw_cwtfactor where tableid = '"+tableId+"' order by columncname");
			for (Map<String, Object> excel_map : excel_list){
				String fields = "";
				String values = "";
				//判断excel中列是否存在
				for (int i = 0;i<factorList.size();i++){
					if(excel_map.get(factorList.get(i).get("COLUMNCNAME").toString()) != null){
						fields += factorList.get(i).get("COLUMNNAME")+",";
						values += ("".equals(excel_map.get(factorList.get(i).get("COLUMNCNAME").toString().trim())) ? null : "'"+excel_map.get(factorList.get(i).get("COLUMNCNAME").toString().trim())+"'" )+",";
					}
				}
				if(!"".equals(fields.trim()))
					fields = fields.substring(0,fields.length()-1);
				if(!"".equals(values.trim()))
					values = values.substring(0,values.length()-1);
				String sql_update = "";
				if(!"".equals(values) && !"".equals(fields)){
					sql_update = "insert into "+tableName+" ("+fields+") values ("+values+")";
					sqls.add(sql_update);
				}
			}
			if(sqls != null){
				String DBINFO = sqlMapperService.selectOne("select t1.dbinfo from bi_t_dbinfo t1 where t1.centrewarehouseid = \n" +
															"(select t2.centrewarehouseid from raw_cwtable t2 where guid = '"+tableId+"')\n",String.class);
				DBInfoBean dbinfo = this.getDBInfoBeanByPipeline("ORACLE", DBINFO);
				dynamicConnServiceV2.execSqls(dbinfo,sqls);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("系统错误<br>" + e.getMessage());
		}
		return result;
	}

	/**
	 * 保存表详情
	 *
	 * @param tableId  贴源库表id
	 * @param annotation 表详情文本
	 *
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/saveAnnotationByTableId")
	public ResultBody saveAnnotationByTableId(String tableId, String annotation) {
		try {
			String sql = "update RAW_CWTABLE set annotation = '" + annotation + "' where guid = '" + tableId + "'";
			int updateSuccessTimes = sqlMapperService.update(sql);
			if(updateSuccessTimes == 1) {
				return ResultBody.createSuccessResult("");
			}else {
				return ResultBody.createErrorResult("保存失败");
			}
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("保存失败 原因："+e.toString());
		}
	}
	/***
	 * 复制贴源库表
	 *
	 * @param request
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/copyRawTable")
	@Description("复制贴源库表")
	public ResultBody copyRawTable(HttpServletRequest request) {
		UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
		String guid = request.getParameter("guid");
		String tableName = request.getParameter("tTableName");   // 新 表物理名称
		String tableCName = request.getParameter("tTableCName"); // 新 表中文名称
		String sTableId = request.getParameter("sTableId"); // 表中文名称
//		String type = request.getParameter("type"); // 是否添加操作
//		String DWTAB_EXPIRATION_TIME = request.getParameter("DWTAB_EXPIRATION_TIME"); // 过期时间
//		String unitVal = request.getParameter("unitVal"); // 数据过期时间单位
//		String saveHistoryData = request.getParameter("SAVEHISTORYDATA"); // 数据过期时间单位
//		String catId = request.getParameter("catId"); // 数据过期时间单位
		String centreWarehouseId = request.getParameter("cwtid"); // 所属库  目标库
//		String remark = request.getParameter("remark"); // 备注

		Map<String, Object> targetPipe = getTargetPipe(centreWarehouseId); // 仓库目标管道
		if (targetPipe == null) {
			return ResultBody.createErrorResult("目标数据管道信息获取失败，请检查！");
		}

		DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());
		try {
			dynamicConnServiceV2.testConn(dbinfo);
		} catch (Exception e1) {
			return ResultBody.createErrorResult("目标数据管道连接失败 </br>" + e1.getMessage());
		}
		try {
			//构造逻辑定义
			//String targetPipeId = targetPipe.get("GUID").toString(); // 目标管道ID

			String newGuid = sqlMapperService.selectOne("select sys_guid() as guid from dual", String.class);

			//复制表定义
			String insertCwtTable = "insert into raw_cwtable" +
					"  (guid,tablename,tablecname,centrewarehouseid,sourcetabname,sourcetabcname,sourcetabtype," +
					"  dbinfoid, creater, drc_versionid,expiration_unit," +
					"  savehistorydata,dwtab_expiration_time,catid,annotation,createuser,createuserid,alteruser," +
					"  alteruserid, sourcepipelineid,formula, resulttable,is_view) " +
					"  (select '"+newGuid+"','"+tableName+"','"+tableCName+"',centrewarehouseid,sourcetabname,sourcetabcname,'01'," +
					"  dbinfoid, creater, drc_versionid,expiration_unit," +
					"  savehistorydata,dwtab_expiration_time,catid,annotation,'"+user.UserName+"','"+user.MID+"',alteruser," +
					"  alteruserid, sourcepipelineid,formula, resulttable,is_view " +
					"  from raw_cwtable where guid = '" + sTableId + "')";
			int insertCwtTableNum = sqlMapperService.insert(insertCwtTable);
			if(insertCwtTableNum != 1) {
				return ResultBody.createErrorResult("操作失败！");
			}

			//复制表字段定义
			String insertCwtfactor = "INSERT INTO RAW_cwtfactor " +
					"(ISPK,COLUMNNAME,COLUMNCNAME, DATATYPE,DATA_LENGTH,DATA_SCALE," +
					"NOTNULL,DATA_DEFAULT, ANNOTATION, TABLEID,ISPARTITION,PARTITION_ORDER,ORDERNUM,INPUTRULES,CREATEUSER,CREATEUSERID) " +
					"(select ISPK,COLUMNNAME,COLUMNCNAME, DATATYPE,DATA_LENGTH,DATA_SCALE," +
					"NOTNULL,DATA_DEFAULT, ANNOTATION, '"+newGuid+"',ISPARTITION,PARTITION_ORDER,ORDERNUM,INPUTRULES,'"+user.UserName+"','"+user.MID+"' " +
					"from raw_cwtfactor where TABLEID = '" + sTableId + "')";
			int insertCwtfactorNum = sqlMapperService.insert(insertCwtfactor);
			if(insertCwtfactorNum < 1) {
				String deleteCwtTable = "delete from raw_cwtable where guid = '" + newGuid + "'";
				sqlMapperService.delete(deleteCwtTable);
				return ResultBody.createErrorResult("操作失败！");
			}
			String selectCwtfactorSql = "select * from raw_cwtfactor where TABLEID = '"+newGuid+"'";
			List<Map<String, Object>> factorList = sqlMapperService.selectList(selectCwtfactorSql);


			// 构建物理表
			ResultBody createCDWTable = this.createCDWTable(dbinfo, tableName, tableCName, factorList, "0");
			if (createCDWTable.isError) {
				if (DBTypeConstant.ORACLE.equals(dbinfo.DBTYPE)) {
					this.dropCDWTable(dbinfo, tableName);
				}
				//如果构建物理表失败， 删除逻辑定义
				String deleteCwtTable = "delete from raw_cwtable where guid = '" + newGuid + "'";
				String deleteCwtfactor = "delete from RAW_cwtfactor where tableid = '" + newGuid + "'";
				ArrayList<String> sqlStringList = new ArrayList<>();
				sqlStringList.add(deleteCwtTable);
				sqlStringList.add(deleteCwtfactor);
				sqlMapperService.execSqls2(sqlStringList);
			}
			// 贴源库表新增 , 建立初始转换
			ResultBody result_createTrans = mfbicommonService.createEmptyTrans("1", centreWarehouseId, tableCName, tableName, user.optAdmdivCode, "");
			if (result_createTrans.isError) {
				this.dropCDWTable(dbinfo, tableName);
				//如果构建物理表失败， 删除逻辑定义
				String deleteCwtTable = "delete from raw_cwtable where guid = '" + newGuid + "'";
				String deleteCwtfactor = "delete from RAW_cwtfactor where tableid = '" + newGuid + "'";
				ArrayList<String> sqlStringList = new ArrayList<>();
				sqlStringList.add(deleteCwtTable);
				sqlStringList.add(deleteCwtfactor);
				sqlMapperService.execSqls2(sqlStringList);
				return result_createTrans;
			}
			Map<String, Object> _map = (Map<String, Object>) result_createTrans.result;
			String transId = _map.get("transId").toString();// 转换id
			// 保存转换信息
			mfbicommonService.create_eltransinfo(user.optAdmdivCode, Integer.parseInt(transId), centreWarehouseId, newGuid, "01", "1", user.UserName, "0");
			// 贴源库表定义存储转换id
			String updateCDW = "update RAW_CWTABLE set TRANSID='" + transId + "' where GUID='" + newGuid + "'";
			// 修改贴源库定义
			sqlMapperService.update(updateCDW);
			return ResultBody.createSuccessResult("");
		} catch (Exception e) {
			e.printStackTrace();
			this.dropCDWTable(dbinfo, tableName);
			return ResultBody.createErrorResult("操作失败！");
		}
	}
	/**
	 * 获取某个贴源库表的血缘关系数据
	 *
	 * @param centrewarehouseId  所属库id
	 * @param tableName 贴源库表名
	 *
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getDLS_RAW_table")
	public  List<Map<String,Object>> getDLS_RAW_table(String centrewarehouseId, String tableName) {
		try {
			return mfbicommonService.getDLS_RelSelf_table(centrewarehouseId,tableName);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 保存来源血缘关系
	 *
	 * @param centrewarehouseId  所属库id
	 * @param tableName 贴源库表名
	 * @param sTableNameListJsonString 要设置的来源表集合的Json字符串
	 *
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/setSourceTable")
	public  ResultBody setSourceTable(String centrewarehouseId, String tableName, String tableCName, String sTableListJsonString) {
		try {
			//Map<String, Object> pipeline = this.getPipeline("9E47F5AA0D496B97E055000000000001");
			Map<String, Object> targetPipe = getTargetPipe(centrewarehouseId);
			DBInfoBean dbinfo = this.getDBInfoBeanByPipeline(targetPipe.get("DBTYPE").toString(), targetPipe.get("DBINFO").toString());
			DBInfoBean dbinfot = dbinfo;
			List<Map<String, Object>> sTableDataList = JSONArray.fromObject(sTableListJsonString);
			if(sTableDataList.size() >= 1) {
				//先删除该表的所有血缘关系
				String deleteDlsSql = "delete from DLS_T_RELATION where tid = 'RAW~!" + centrewarehouseId + "~!" + tableName + "'";
				sqlMapperService.execSql2(deleteDlsSql);
			}
			for (Map<String, Object> sTableInfoMap : sTableDataList) {
				Map<String, Object> sTableInfo = new HashMap<String, Object>();
				sTableInfo.put("TABLE_NAME", sTableInfoMap.get("TABLENAME"));
				sTableInfo.put("TABLE_CNAME", sTableInfoMap.get("TABLECNAME"));
				sTableInfo.put("tableName", tableName);
				sTableInfo.put("tableCName", tableCName);
				// 保存血缘关系数据
				mfbicommonService.saveDLS_RelRaw_RawToRaw_SetSource(centrewarehouseId, sTableInfo, dbinfo, dbinfot);
			}
			return ResultBody.createSuccessResult("");
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult("");
		}
	}

	/**
	 * 数据采集界面设置采集方式中取消支持手工录入
	 *
	 * @param centreWarehouseId  所属库id
	 * @param tableId 贴源库表id
	 *
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/cancelRecordManuallyResult")
	public  ResultBody cancelRecordManuallyResult(String tableId) {
		try {
			String sql = "delete from RAW_ETL_TABLECONFIG where tableid = '" + tableId + "' and collecttype = '02' ";
			sqlMapperService.execSql2(sql);
			return ResultBody.createSuccessResult("");
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult(e.toString());
		}
	}

	/**
	 * 获取所有贴源库
	 *
	 * @param request
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getAllRAWCentrewareHouse")
	public List<Map<String, Object>> getAllRAWTable(HttpServletRequest request) {
		try {
			UserBean user = (UserBean) request.getSession().getAttribute("CurUser");
			String getAllCategory = "select * from raw_datacategory where PROVINCE='" + user.optAdmdivCode + "' order by cname";
			List<Map<String, Object>> allCategoryList = sqlMapperService.selectList(getAllCategory);
			StringBuffer categoryIdsSql = new StringBuffer("");
			allCategoryList.forEach(map->{
				categoryIdsSql.append("'").append(map.get("ID").toString()).append("',");
			});
			String getAllRAWCentrewareHouseByAllCategory = "";
			List<Map<String, Object>> RAWCentrewareHouseList = new ArrayList<>();
			if(categoryIdsSql.length() > 0) {
				categoryIdsSql.deleteCharAt(categoryIdsSql.length() - 1);
				getAllRAWCentrewareHouseByAllCategory = "select GUID,REMARK,CNAME,DATACATEGORYID,CREATER,SOURCETYPE, to_char(createdate,'yyyy-mm-dd  hh24:mm:ss' ) as CREATEDATE from RAW_centrewarehouse where DATACATEGORYID in ("
						+ categoryIdsSql + ") ORDER BY CREATEDATE asc, CNAME";
				RAWCentrewareHouseList = sqlMapperService.selectList(getAllRAWCentrewareHouseByAllCategory);
				if(!(RAWCentrewareHouseList.size() == 0)) {
					return RAWCentrewareHouseList;
				}
			}
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 根据仓库id获取该仓库下的所有贴源库表，无分页
	 *
	 * @param centreWarehouseId
	 *            库id
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getTableListNoPage")
	public List<Map<String, Object>> getTableListNoPage(HttpServletRequest request) {
		try {
			String CENTREWAREHOUSEID = request.getParameter("CENTREWAREHOUSEID");
			String sql = "select * from RAW_CWTABLE where CENTREWAREHOUSEID = '" + CENTREWAREHOUSEID + "'";
			List<Map<String, Object>> RAWTableListByCentrewareHouseList = sqlMapperService.selectList(sql);
			return RAWTableListByCentrewareHouseList;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 给中心仓库表设置来源血缘关系，贴源库 --> 中心仓库
	 *
	 * @param sdatabaseid
	 * 来源表库id
	 * @param tdatabaseid
	 * 目标贴源库表
	 * @param checkTableJsonString
	 * 来源血缘关系表json字符串
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/setSourceTableCWToRAW")
	public ResultBody setSourceTableCWToRAW(HttpServletRequest request) {
		try {
			//删除原有血缘关系
			String sdatabaseid = request.getParameter("sdatabaseid");
			String tdatabaseid = request.getParameter("tdatabaseid");
			String tABLENAME = request.getParameter("tABLENAME");
			String tABLECNAME = request.getParameter("tABLECNAME");
			String checkTableJsonString = request.getParameter("checkTableJsonString");
			List<Map<String, String>> tables_s = JSONArray.fromObject(checkTableJsonString);
			String appid = MfbiCommon.APPID_CW;
			String deleteDlsSql = "delete from DLS_T_RELATION where TID = '" + appid + "~!" + tdatabaseid + "~!" + tABLENAME + "' and TTYPE='TABLE'";
			sqlMapperService.execSql2(deleteDlsSql);
			tables_s.forEach(sTableMap->{
				mfbicommonService.saveDLS_RelFact_RawToCw(sdatabaseid, tdatabaseid, sTableMap.get("TABLENAME"), sTableMap.get("TABLECNAME"), tABLENAME, tABLECNAME);
			});
			return ResultBody.createSuccessResult("保存成功");
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult(e.toString());
		}
	}

	//爬虫采集数据方法
	/*public void seleniumGetData(String url, String xpath) throws InterruptedException, IOException {
		System.setProperty("webdriver.chrome.driver", "c://chromedriver.exe");
		ChromeOptions chromeOptions = new ChromeOptions();
		chromeOptions.addArguments("--headless");
		WebDriver webDriver = new ChromeDriver(chromeOptions);
		//火狐Driver
//        System.setProperty("webdriver.gecko.driver", "c://geckodriver.exe");
//        WebDriver webDriver = new FirefoxDriver();
		webDriver.manage().window().maximize();
		webDriver.manage().deleteAllCookies();
		// 与浏览器同步非常重要，必须等待浏览器加载完毕
		webDriver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);

		//打开目标地址
//        webDriver.get("file:///D:/workspace/JavaProject/testSelenium/src/main/java/com/zhanglubin/test-selenium.html");
		webDriver.get(url);

		List<WebElement> elements = webDriver.findElements(By.xpath(xpath));

//        List<WebElement> elements = webDriver.findElements(By.cssSelector("td"));
//        List<WebElement> elements2 = webDriver.findElements(By.cssSelector("td[style = 'width: 80pt; font-size: 11.0pt; font-family: 微软雅黑, sans-serif; text-align: center; vertical-align: middle; white-space: normal; color: windowtext; font-weight: 400; font-style: normal; text-decoration: none; border-left: medium none; border-right: .5pt solid windowtext; border-top: medium none; border-bottom: .5pt solid windowtext; padding-left: 1px; padding-right: 1px; padding-top: 1px; background: white']"));
		for (WebElement element : elements) {
			System.out.println(element.getText());
		}
		webDriver.quit();
	}*/

	/**
	 * 业务库对注册表一致性检查，表结构是否发生变动
	 * @param rawGuid
	 */
	@ResponseBody
	@RequestMapping("/consistencyCheck")
	public ResultBody consistencyCheckRegTable(String rawGuid){

		try {
			List<String> sqls = new ArrayList<>(); // 逻辑定义标记变更信息
			// 取出业务库管道信息
			Map<String, Object> rawInfo = mfbicommonService.getRawInfo(rawGuid);
			DBInfoBean dbinfo = mfbicommonService.getDBInfoBeanByPipeline(rawInfo.get("DBINFO").toString());

			// 业务库逻辑字段信息
			String sql = "SELECT t.guid,t.tablename,t.regtablechg,(SELECT COUNT(1) FROM Raw_Cwtfactor t1 WHERE t1.tableid = t.guid) colCnt from Raw_Cwtable t WHERE t.centrewarehouseid='"+rawGuid+"'";
			List<Map<String, Object>> regTableInfo = sqlMapperService.selectList(sql);

			// 业务库真实字段
			String sql_ = " SELECT t.TABLE_NAME tablename,(SELECT COUNT(1) FROM USER_TAB_COLUMNS t1 WHERE t1.table_name = t.TABLE_NAME) colCnt from user_tab_comments t";
			List<Map<String, Object>> factCnt = dynamicConnServiceV2.selectList2(dbinfo,sql_);

			Map<String,Integer> defnTableCols = new HashedMap();
			for(Map<String, Object> m1 : factCnt)
				defnTableCols.put(m1.get("TABLENAME").toString(),Integer.valueOf(m1.get("COLCNT").toString()));
			for(Map<String, Object> m2 : regTableInfo){
				if(defnTableCols.get(m2.get("TABLENAME")) == null)
					continue;
				int cnt1 = defnTableCols.get(m2.get("TABLENAME")); // 物理表中
				int cnt2 = Integer.valueOf(m2.get("COLCNT").toString()); // 逻辑定义中
				// 逻辑定义列数与物理字段数不相同的情况
				if(cnt1 != cnt2)
					sqls.add("update Raw_Cwtable set REGTABLECHG='1' where guid='"+m2.get("GUID")+"'");
			}
			if(sqls.size() > 0){
				sqlMapperService.execSqls2(sqls);
			}
			return ResultBody.createSuccessResult(sqls.size());
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult(e.getMessage());
		}

	}

	/**
	 * 获取注册字段与物理表字段差异
	 * @param rawGuid
	 * @param tableGuid
	 * @param tableName
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getFieldMapping_raw")
	public ResultBody getFieldMapping_raw(String rawGuid,String tableGuid,String tableName){

		try {
			// 取出业务库管道信息
			Map<String, Object> rawInfo = mfbicommonService.getRawInfo(rawGuid);
			DBInfoBean dbinfo = mfbicommonService.getDBInfoBeanByPipeline(rawInfo.get("DBINFO").toString());

			// 业务库逻辑字段信息
			String sql = "SELECT t.guid,t.columnname,t.columncname from raw_cwtfactor t WHERE t.tableid='"+tableGuid+"'";
			List<Map<String, Object>> regTableInfo = sqlMapperService.selectList(sql);

			// 业务库真实字段
			String sql_ = " SELECT COLUMN_NAME FROM USER_TAB_COLUMNS t1 WHERE t1.table_name = '"+tableName+"'";
			List<Map<String, Object>> factCnt = dynamicConnServiceV2.selectList2(dbinfo,sql_);
			Map<String,String> defnTableCols = new HashedMap();
			for(Map<String, Object> m1 : factCnt)
				defnTableCols.put(m1.get("COLUMN_NAME").toString(),m1.get("COLUMN_NAME").toString());

			List<Map<String, Object>> data = new ArrayList<>();
			Map<String,Object> root1 = new HashedMap();// 相同字段
			root1.put("ID","root1");
			root1.put("PID",null);
			root1.put("NAME","无差异字段信息");
			data.add(root1);
			Map<String,Object> root2 = new HashedMap();// 不同字段
			root2.put("ID","root2");
			root2.put("PID",null);
			root2.put("NAME","有差异字段信息");
			data.add(root2);

			for(Map<String, Object> m2 : regTableInfo) { // 注册字段查找物理表字段
				Map<String,Object> row = new HashedMap();
				row.put("ID",m2.get("COLUMNNAME").toString());
				row.put("COLUMNNAME",m2.get("COLUMNNAME").toString());
				//  注册字段在物理表定义中找不到的，属于物理表中已删除
				if (defnTableCols.get(m2.get("COLUMNNAME")) == null) {
					row.put("PID","root2");
					row.put("TYPE","1");
					row.put("COLUMNNAME","不存在");
					row.put("COLUMNNAME_REG",m2.get("COLUMNNAME").toString());
					data.add(row);
				} else {
					row.put("PID","root1");
					row.put("COLUMNNAME",m2.get("COLUMNNAME").toString());
					row.put("COLUMNNAME_REG",m2.get("COLUMNNAME").toString());
					data.add(row);
					defnTableCols.remove(defnTableCols.get(m2.get("COLUMNNAME")));
				}
			}
			if(!defnTableCols.isEmpty()){ // 物理定义中存在多余的，即注册信息中缺失的
				for(String key : defnTableCols.keySet()){
					Map<String,Object> row = new HashedMap();
					row.put("ID",key);
					row.put("TYPE","2");
					row.put("COLUMNNAME_REG","缺失");
					row.put("COLUMNNAME",key);
					row.put("PID","root2");
					data.add(row);
				}
			}
			return ResultBody.createSuccessResult(data);
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult(e.getMessage());
		}

	}

	/**
	 * 同步物理表字段注册信息
	 * @param rawGuid
	 * @param tableGuid
	 * @param tableName
	 * @param rows 存在差异的字段
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/syncTableDefn_reg")
	public ResultBody syncTableDefn_reg(String rawGuid,String tableGuid,String tableName,String rows){

		try {
			List<Map<String, String>> cols = JSONArray.fromObject(rows);
			// 取出业务库管道信息
			Map<String, Object> rawInfo = mfbicommonService.getRawInfo(rawGuid);
			DBInfoBean dbinfo = mfbicommonService.getDBInfoBeanByPipeline(rawInfo.get("DBINFO").toString());

			// 第一种情况，物理表字段有增加，需要补充注册字段
			// 第二种情况，物理表字段有删除，需要删除注册字段
			List<String> sqls = new ArrayList<>(); // 删除和增加字段逻辑定义
			Map<String,Object> filterCols = new HashedMap();
			for(Map<String, String>  map : cols){
				if(map.get("TYPE") == null)
					continue;
				if("1".equals(map.get("TYPE"))){ // 注册信息需要删除
					sqls.add("delete raw_cwtfactor where tableid='"+tableGuid+"' and columnname='"+map.get("COLUMNNAME_REG")+"'");
				}else{ // 补充注册信息
					filterCols.put(map.get("COLUMNNAME"),map.get("COLUMNNAME"));
				}
			}
			if(!filterCols.isEmpty()){
				List<Map<String, Object>> columns = getCWFactorsList(dbinfo, tableName, JSONObject.fromObject(filterCols).toString());
				for(Map<String, Object> m : columns)
					this.addRAW_cwtfactor(sqls,null,dbinfo,m,0,tableGuid);
			}
			sqls.add("UPDATE RAW_CWTABLE SET REGTABLECHG='0' WHERE GUID='"+tableGuid+"'");
			sqlMapperService.execSqls2(sqls);
			return ResultBody.createSuccessResult(null);
		} catch (Exception e) {
			e.printStackTrace();
			return ResultBody.createErrorResult(e.getMessage());
		}

	}

	/**
	 * 爬虫
	 * @param tableId 表id
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getReptile")
	public List<Map<String, Object>> getReptile(String tableId,String rawId){
		List<Map<String, Object>> reptileList = sqlMapperService.selectList("select connectionid from RAW_ETL_TABLECONFIG where rawid = '" + rawId + "' and tableid = '" + tableId + "' and collecttype = '04' ");
		return reptileList;
	}

	@ResponseBody
	@RequestMapping("/getCollectTables")
	public List<Map<String, Object>> getCollectTables(String rawid,String collectType){
		return sqlMapperService.selectList("select * from raw_etl_tableconfig where rawid='" + rawid + "' and collecttype = '" + collectType + "'");
	}
}
