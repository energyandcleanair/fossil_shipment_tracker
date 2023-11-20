 delete from trajectory;
 delete from shipmentdepartureberth;
 delete from shipmentarrivalberth;
 delete from shipment;
 delete from shipment_with_sts;
 delete from arrival;
 with deleted_departures as (
 delete from departure
 )
 SELECT count(id)
 FROM departure;
