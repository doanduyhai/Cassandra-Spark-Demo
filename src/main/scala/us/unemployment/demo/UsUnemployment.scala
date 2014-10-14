package us.unemployment.demo

case class UsUnemployment(year: Int, civilNonInstitutionalCount: Int, civilLaborCount: Int, laborPopulationPercentage: Double,
                          employedCount: Int, employedPercentage: Double, agriculturePartCount: Int, nonAgriculturePartCount: Int, UnemployedCount: Int,
                          unemployedPercentageToLabor: Double, notLaborCount: Int) {

  override def toString() = s"""Year($year), unemployment % : $unemployedPercentageToLabor"""
}
