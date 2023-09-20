package sh.ctr.common

object Constant {

  val PLATFORM_APP = "app"

  val APP_ID_MAP: Map[String, Array[String]] = Map[String, Array[String]](
    PLATFORM_APP -> Array[String]("news", "newssdk")
  )

  val TABLE_TRACKING_CHARGE_D = "mbbi.t_dwd_xps2_trackingcharge_d"
  val TABLE_IMP_CLICK = "mbshctr.t_ods_xps_imp_click"

  val TABLE_STATS_SLICE = "mbshctr.t_dwt_xps_stats_slice"
  val TABLE_STATS_SLICE_TMP = "v_dwt_xps_stats_slice"

  val TABLE_USER_ACTIONS = "mbshctr.t_dwt_xps_user_actions"
  val TABLE_USER_ACTIONS_TMP = "v_dwt_xps_user_actions"

  val TABLE_STATS_FEAS = "mbshctr.t_ads_xps_stats_feas"
  val TABLE_STATS_FEAS_TMP = "v_ads_xps_stats_feas"

  val TABLE_USER_ACTION_FEAS = "mbshctr.t_ads_xps_user_action_feas"
  val TABLE_USER_ACTION_FEAS_TMP = "v_ads_xps_user_action_feas"

  val BASE_PATH_TEST: String = "/user/mxbps/algo/xgb_test"

  val BASE_PATH_XGB = "/user/mbshctr/algo/xgb"
}
