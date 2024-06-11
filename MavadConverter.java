package net.navoshgaran.mavad;

import java.sql.Connection;


import net.navoshgaran.mavad.cnv.*;
//import net.itorbit.citadel.nrm.cnv.Trigger;

public class MavadConverter {
    private static String sourceSchema = "ANPDEVELOPER";
    //	private static String sourceSchema="MAVAD";
//	private static String targetSchema="ANPDEVELOPER";
    private static String targetSchema = "MAVAD";
    private static String Camunda = "CAMUNDA";
    public static int domainIdx = 4442;//6823;//tb_domains.dom_id
    public static int startIdx = 30000000;
    public static int systemIdx = 4441;//6825;//tb_systems.sys_id
    public static int domSystemIdx = 4443;//tb_domsystems.doms_id
    public static long LICRoleIdx = startIdx - 3;
    public static long AllURLsSOBJIdx = startIdx - 4;
    public static long AllURLsRoleAccessIdx = startIdx - 5;
    public static final String HOURS = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
    public static final String MACHINES = "*.*.*.*";

    public static void main(String[] args) throws Exception {

        IdGenerator idGenerator = new IdGenerator(startIdx);
//		Preprocessor preprocessor = new Preprocessor(sourceSchema);
//		if (!preprocessor.run())
//			System.exit(1);
        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        Connection connection = connectionFactory.getConnection();
        Connection connectionTarget = connectionFactory.getConnectionTarget();
        Connection connectionCamunda = connectionFactory.getConnectioncamunda();

        connection.setAutoCommit(false);
        try {

//			MavadUser mavadUser = new MavadUser(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget,connectionCamunda);
//			mavadUser.run();
//			java.lang.System.out.println("Portal-User And ACT_ID_USER Table filled!");

//			MavadJob mavadJob = new MavadJob(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget,connectionCamunda);
//			mavadJob.run();
//			java.lang.System.out.println("Mavad job Table filled!");


//			MavadEmployee employee = new MavadEmployee(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget,connectionCamunda);
//			employee.run();
//			java.lang.System.out.println("Employee And job Table filled!");

//			MavadCountry mavadCountry = new MavadCountry(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadCountry.run();
//			System.out.println("Country Table filled!");

//			MavadProvince province = new MavadProvince(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget,connectionCamunda);
//			province.run();
//			java.lang.System.out.println("province Table filled!");

//			MavadCITY city  = new MavadCITY(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget,connectionCamunda);
//			city.run();
//			java.lang.System.out.println("city Table filled!");

//			MavadInventory  mavadInventory  = new MavadInventory(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadInventory.run();
//			java.lang.System.out.println("Inventory filled!");

//			MavadDrug mavadDrug  = new MavadDrug(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget,connectionCamunda);
//			mavadDrug.run();
//			java.lang.System.out.println("Drug table filled!");

//			MavadMeasure mavadMeasure  = new MavadMeasure(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget,connectionCamunda);
//			mavadMeasure.run();
//			java.lang.System.out.println("Measure table filled!");

//			MavadVaccine mavadVaccine  = new MavadVaccine(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadVaccine.run();
//			java.lang.System.out.println("MavadVaccine filled!");

//			MavadBreed mavadBreed = new MavadBreed(targetSchema, sourceSchema, idGenerator, Camunda, connection, connectionTarget, connectionCamunda);
//			mavadBreed.run();
//			java.lang.System.out.println("MavadBreed Table filled!");

//			MavadDogInformation mavadDogInformation = new MavadDogInformation(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadDogInformation.run();
//			java.lang.System.out.println("Dog Information filled!");

//			MavadDogInformationVaccine mavadDogInformationVaccine = new MavadDogInformationVaccine(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadDogInformationVaccine.run();
//			System.out.println("Dog Information vaccine table filled!");

//			MavadAppointment mavadAppointment =  new MavadAppointment(targetSchema, sourceSchema, idGenerator,connection,connectionTarget);
//			mavadAppointment.run();
//			System.out.println("Dog Appointment table filled!");

//			MavadCoachTrainingCourse mavadCoachTrainingCourse = new MavadCoachTrainingCourse(targetSchema, sourceSchema, idGenerator,connection,connectionTarget);
//			mavadCoachTrainingCourse.run();
//			System.out.println("Coach Training Course table filled!");

//			MavadDogTrainingCourse mavadDogTrainingCourse = new MavadDogTrainingCourse(targetSchema, sourceSchema, idGenerator,connection,connectionTarget);
//			mavadDogTrainingCourse.run();
//			System.out.println("Dog Training Course table filled!");

//			MavadInstruction mavadInstruction = new MavadInstruction(targetSchema, sourceSchema, idGenerator,connection,connectionTarget);
//			mavadInstruction.run();
//			System.out.println("Instruction table filled!");

//			MavadCoachTraining mavadCoachTraining = new MavadCoachTraining(targetSchema, sourceSchema, idGenerator,connection,connectionTarget);
//			mavadCoachTraining.run();
//			System.out.println("Coach Training table filled!");

//			MavadCoachTrainingEmployee mavadCoachTrainingEmployee = new MavadCoachTrainingEmployee(targetSchema, sourceSchema, idGenerator,connection,connectionTarget);
//			mavadCoachTrainingEmployee.run();
//			System.out.println("Coach Training Employee table filled!");

//			MavadDogReq mavadDogReq = new MavadDogReq(targetSchema, sourceSchema, idGenerator,connection,connectionTarget);
//			mavadDogReq.run();
//			System.out.println("Dog Req table filled!");

//            MavadDogDelivery mavadDogDelivery = new MavadDogDelivery(targetSchema, sourceSchema, idGenerator, connection, connectionTarget);
//            mavadDogDelivery.run();
//            System.out.println("Dog Req table filled!");

//			MavadBank mavadBank = new MavadBank(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget,connectionCamunda);
//			mavadBank.run();
//			java.lang.System.out.println("Bank table filled!");

//			MavadDogEquipmentType mavadDogEquipmentType = new MavadDogEquipmentType(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadDogEquipmentType.run();
//			java.lang.System.out.println("Dog Equipment Type table filled!");

//			MavadLaboratory laboratory = new MavadLaboratory(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			laboratory.run();
//			System.out.println("Laboratory table is filled.");

//			MavadProductGroup mavadProductGroup = new MavadProductGroup(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadProductGroup.run();
//			System.out.println("Product_Group table is filled.");

//			MavadProduct mavadProduct = new MavadProduct(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadProduct.run();
//			System.out.println("Product table is filled.");

//			MavadUnitsType mavadUnitsType = new MavadUnitsType(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadUnitsType.run();
//			System.out.println("UnitsType table is filled.");

			MavadUnits mavadUnits = new MavadUnits(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
			mavadUnits.run();
            System.out.println("Units is filled.");


//			MavadZonePartition mavadZonePartition = new MavadZonePartition(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadZonePartition.run();
//			System.out.println("Zone Partition table is filled.");

//			MavadPoliceGrade mavadPoliceGrade = new MavadPoliceGrade(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadPoliceGrade.run();
//			System.out.println("Police Grade table is filled.");

//			MavadVehicleType mavadVehicleType = new MavadVehicleType(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadVehicleType.run();
//			System.out.println("Vehicle Type table is filled.");

//			MavadVehicleColor mavadVehicleColor = new MavadVehicleColor(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadVehicleColor.run();
//			System.out.println("Vehicle Color table is filled.");

//			MavadVehicleBrand mavadVehicleBrand = new MavadVehicleBrand(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadVehicleBrand.run();
//			System.out.println("Vehicle Brand table is filled.");

//			MavadVehicleClass mavadVehicleClass = new MavadVehicleClass(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadVehicleClass.run();
//			System.out.println("Vehicle Class table is filled.");

//			MavadSubjectPlan mavadSubjectPlan = new MavadSubjectPlan(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadSubjectPlan.run();
//			System.out.println("Subject Plan table is filled.");

//			MavadDrugItem mavadDrugItem = new MavadDrugItem(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadDrugItem.run();
//			System.out.println("Drug Item table is filled.");

//			MavadCriminalRole mavadCriminalRole = new MavadCriminalRole(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadCriminalRole.run();;
//			System.out.println("Criminal Role Item table is filled.");

//			MavadFolder mavadFolder = new MavadFolder(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadFolder.run();
//			System.out.println("Folder table is filled.");

//            MavadManageEnforcementPlan mavadManageEnforcementPlan = new MavadManageEnforcementPlan(targetSchema, sourceSchema, idGenerator, connection, connectionTarget);
//            mavadManageEnforcementPlan.run();
//            System.out.println("Manage Enforcement Plan table is filled.");

//			MavadInspectionMeetingNote mavadInspectionMeetingNote = new MavadInspectionMeetingNote(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadInspectionMeetingNote.run();
//			System.out.println("Inspection Meeting Note table is filled.");

//			MavadInspectionMeetingNoteWithoutFolder mavadInspectionMeetingNoteWithoutFolder
//					= new MavadInspectionMeetingNoteWithoutFolder(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadInspectionMeetingNoteWithoutFolder.run();
//			System.out.println("InspectionMeetingNoteWithoutFolder is filled.");

//            MavadManageEnforcementInspection mfi = new MavadManageEnforcementInspection(targetSchema, sourceSchema, idGenerator, connection, connectionTarget);
//            mfi.run();
//            System.out.println("Manage Enforcement Inspection updated.");

//			MavadPresentMember mavadPresentMember =  new MavadPresentMember(targetSchema, sourceSchema, idGenerator,connection,connectionTarget);
//			mavadPresentMember.run();
//			System.out.println("PresentMember table is filled.");

//			MavadVehicle mavadVehicle = new MavadVehicle(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadVehicle.run();
//			System.out.println("Vehicle table is filled.");

//			MavadAccused mavadAccused = new MavadAccused(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadAccused.run();
//			System.out.println("Accused table is filled.");

//			MavadAccused2 mavadAccused2 =  new MavadAccused2(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadAccused2.run();
//			System.out.println("Accused2 table is filled.");

//			MavadGunType mavadGunType = new MavadGunType(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadGunType.run();
//			System.out.println("Gun table is filled.");

//			MavadDiscoveryWeapon mavadDiscoveryWeapon = new MavadDiscoveryWeapon(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadDiscoveryWeapon.run();
//			System.out.println("Discovery Weapon table is filled.");

//			MavadInventoryItem mavadInventoryItem = new MavadInventoryItem(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadInventoryItem.run();
//			System.out.println("Inventory Item table is filled.");

//			MavadInventoryItem2 mavadInventoryItem2 = new MavadInventoryItem2(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadInventoryItem2.run();
//			System.out.println("Inventory Item table filled");

//			MavadEmbedType mavadEmbedType =  new MavadEmbedType(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadEmbedType.run();
//			System.out.println("Embed_Type table is filled.");

//			MavadEmbedMethod mavadEmbedMethod = new MavadEmbedMethod(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadEmbedMethod.run();
//			System.out.println("Embed_Method table is filled.");

//			MavadDiscoveryNarcotic mavadDiscoveryNarcotic = new MavadDiscoveryNarcotic(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadDiscoveryNarcotic.run();
//			System.out.println("Discovery Narcotic insert into inventory item table is filled.");

//			MavadDiscoveryCoinAndCash mavadDiscoveryCoinAndCash = new MavadDiscoveryCoinAndCash(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget);
//			mavadDiscoveryCoinAndCash.run();
//			System.out.println("Discovery Coin And Cash table is filled.");

//			MavadAnotherPresentMember mavadAnotherPresentMember = new MavadAnotherPresentMember(targetSchema, sourceSchema, idGenerator,connection,connectionTarget);
//			mavadAnotherPresentMember.run();
//			System.out.println("member life damage table is filled.");

//			MavadPerson mavadPerson = new MavadPerson(targetSchema, sourceSchema,idGenerator,connection,connectionTarget);
//			mavadPerson.run();
//			System.out.println("Nap_person table is filled.");

//			MavadTelephone mavadTelephone = new MavadTelephone(targetSchema, sourceSchema,idGenerator,connection,connectionTarget);
//			mavadTelephone.run();
//			System.out.println("Nap_Telephone table is filled.");

//			MavadBankAccount mavadBankAccount = new MavadBankAccount(targetSchema, sourceSchema,idGenerator,connection,connectionTarget);
//			mavadBankAccount.run();
//

//            MavadPLanGroup mavadPLanGroup = new MavadPLanGroup(targetSchema, sourceSchema,idGenerator,connection,connectionTarget);
//            mavadPLanGroup.run();
//            System.out.println("Nap_PLAN_GROUP table is filled.");

//            MavadRegistryAndDiscoveryBonus mavadRegistryAndDiscoveryBonus = new MavadRegistryAndDiscoveryBonus(targetSchema, sourceSchema,idGenerator,connection,connectionTarget);
//            mavadRegistryAndDiscoveryBonus.run();
//            System.out.println("Registry And Discovery Bonus table is filled.");


//            MavadCaseReward caseReward = new MavadCaseReward(targetSchema, sourceSchema, idGenerator, connection, connectionTarget);
//            caseReward.run();
//            System.out.println("Case Reward table is filled.");

//			MavadStaffReward mavadStaffReward = new MavadStaffReward(targetSchema, sourceSchema,idGenerator,connection,connectionTarget);

            MavadReporterPerson mavadReporterPerson =  new MavadReporterPerson(targetSchema, sourceSchema,idGenerator,connection,connectionTarget);
            mavadReporterPerson.run();
            System.out.println("Reporter Person table is filled.");


//			MavadClue mavadclue  = new MavadClue(targetSchema, sourceSchema, idGenerator,Camunda, connection,connectionTarget,connectionCamunda);
//			mavadclue.run();
//			java.lang.System.out.println("mavadClue And job Table filled!");


            connection.commit();
            java.lang.System.out.println("Finish Successfully!!!");

        } catch (Exception ex) {
            ex.printStackTrace();
            connection.rollback();
        } finally {
            connectionFactory.closeConnection(connection);
            connectionFactory.closeConnection(connectionTarget);
            connectionFactory.closeConnection(connectionCamunda);
        }

    }
}
