package es.dmr.uimp.realtime

object Model {

  case class Purchase(invoiceNo: String, quantity: Int, invoiceDate: String,
                      unitPrice: Double, customerID: String, country: String)

  case class Invoice(invoiceNo: String = "", avgUnitPrice: Double = 0.0,
                     minUnitPrice: Double = Double.MaxValue, maxUnitPrice: Double = 0.0, time: Double = 0.0,
                     numberItems: Double = 0L, lastUpdated: Long = 0L, lines: Int = 0, customerId: String = "")

}
