/*Astuce */
/*pour renommer une table*/
ALTER TABLE Librairie.Dossier RENAME TO Librairie2.Dossier

/*Pour changer le type de format d'une variable ic en bigint*/

alter table default.test change variable1 variable1newformat bigint

/*Somme des CA sur 3 tables */
SELECT sum(ca),ref_acq  FROM 

( 
    SELECT ref_acq,ca 
    from default.damglobale_dec17
    UNION ALL 
    SELECT ref_acq,ca 
    FROM default.damglobale_nov17
    UNION ALL
    SELECT ref_acq,ca
    FROM default.damglobale_oct17
	
) t
	WHERE ref_acq in("12869","16188","15589","19870","20041","30002","30003","30004","30006","30056","30066","30076","45539","73765")
	GROUP BY ref_acq
   
/**/
/* on change le format de la date*/
to_timestamp(concat("20", substring(aamm, 1, 2), "/",  substring(aamm, 3, 2), "/01"), "yyyy/MM/dd") as aamm1,/*on transforme en timestamp la donnée*/

/*pour creeer un classement*/
RANK() OVER (ORDER BY montant_euro DESC) as xRank_montant,
RANK() OVER (ORDER BY nb_transactions DESC) as xRank_nbtransactions

/*date*/
WHERE aamm >=${aamm} 

/*Pr compter un nombre de valeurs différentes
exemple compter le nombre de mcc différents*/
select count(distinct(mcc)) from pdv.dam01_view

/*pour faire des jointures entre tables*/
select * from pdv.dam01_view

/


























Create table default.fer_requete1_1 as 
select 
    *,
    case
        when bqe_pay != "FR" THEN "99"
        else "FR"
    End as "departement"
FROM
( SELECT 
    sum_brut_soustraction,
    aamm,
    lib_contact_id_corrige,
    ert,
    srt,
    nbfact_DAM,
    ref_acq,
    res_id,
	Reseau,
	mbr_acq,
	bqe_rsn,
	bqe_esg,
	bqe_lct,
	bqe_pay,
	bqe_naf_val_old,
	bqe_naf_val_new,
	bqe_mcc,
	bqe_dep_n3

FROM 
(SELECT 
    sum_brut_soustraction,
    aamm,
    lib_contact_id_corrige,
    ert,
    srt,
    nbfact_DAM,
    ref_acq,
    res_id,
	Reseau,
	mbr_acq,
    to_timestamp(concat("20", substring(aamm, 1, 2), "/",  substring(aamm, 3, 2), "/01"), "yyyy/MM/dd") as aamm1,
	concat( "01","/",  substring(aamm, 3, 2),"/","20", substring(aamm, 1, 2)) as dam_date
FROM (
        SELECT

            sum(mt_brut_soustraction) as sum_brut_soustraction,
            aamm ,
            ert,
            familleert,
            ref_acq,
            sum(nb_facture) AS nbfact_DAM,
            srt,
            contactid,
            res_id,
			Reseau,
			mbr_acq
        FROM
            (
                SELECT 
                    ((mt_brut_fact)-(mt_annul_fact)) as mt_brut_soustraction,
                    aamm,
                    ert,
                    mbr_acq,
					ref_acq,
                    srt,
                    contactid,
                    nb_facture,
                    res_id,
					CASE
						WHEN ert IN ("20","21","22","24","26","27","28","64","B1") THEN "VAD"
						WHEN ert IN("00","10","30","40","41","42","43","45","46","47","48","49","50","52","54","57","60","65","75","80","92") THEN "PROXI"
						else "autre"
					END AS familleert,
					CASE 
						WHEN res_id="1" then "CB"
						WHEN res_id="2" then "Visa"
						WHEN res_id="3" then "MasterCard"
					END AS Reseau
				FROM pdv.dam01_view 
                
                
            )a1
            GROUP BY aamm,ert,familleert,ref_acq,srt,contactid,res_id,Reseau,mbr_acq
     ) a2
    
            INNER JOIN 
            DEFAULT.fer_type_contact_corrige AS t3
                ON a2.contactid=t3.contact_id AND a2.familleert=t3.famille_ert
    )a3
    INNER JOIN
pdv.ppdv01
ON mbr_acq=bqe_mbr_acq
			where aamm1 >=date_sub(now(),INTERVAL 1 YEAR)
) a4
;
			































            select * from 
(select 
    fusion,
    sum(mt_brut_fact) as mt_brut_fact,
    sum(nb_facture) as nb_facture,
    Reseau,
    contactid,
    case 
        when famille_ERT="PROXI" and contactid="0" then "carte contact"
        when famille_ERT="PROXI" and contactid="1" then "carte sans contact"
        when famille_ERT="PROXI" and contactid="2" then "Mobile sans contact"
        when famille_ERT="PROXI" and contactid='3' then 'carte contact'
        when famille_ERT="VAD"   and contactid="0" then 'VAD'
        when famille_ERT="VAD"   and contactid="1" then 'VAD'
        when famille_ERT="VAD"   and contactid='2' then 'Mobile VAD'
        when famille_ERT="VAD"   and contactid='3' then 'VAD'
        when famille_ERT="NULL"   and contactid="0" then 'NULL'
        when famille_ERT="NULL"   and contactid="1" then 'NULL'
        when famille_ERT="NULL"   and contactid='2' then 'NULL'
        when famille_ERT="NULL"   and contactid='3' then 'NULL'
    end as type_contact_corrige
FROM
(select 
    substr(aamm,3, 2) as mois, substr(aamm,1, 2) as year,
    concat('01','/',substr(aamm,3, 2), '/', '20',  substr(aamm,1, 2)) as fusion,
    sum(mt_brut_fact) as mt_brut_fact,
    sum(nb_facture) as nb_facture,
    res_id,
    contactid,
    CASE 
        WHEN res_id="1" then "CB"
        WHEN res_id="2" then "Visa"
        WHEN res_id="3" then "MasterCard"
    END AS Reseau,
    CASE
        WHEN ert IN ("20","21","22","24","26","27","28","64","B1") THEN "VAD"
        WHEN ert IN("00","10","30","40","41","42","43","45","46","47","48","49","50","52","54","57","60","65","75","80","92") THEN "PROXI"
    END AS Famille_ERT,
    aamm
FROM pdv.dam01_view 
WHERE aamm >=${aamm} 
group by 
    res_id,
    aamm,
    famille_ERT,
    contactid ) a1a
group by 
    fusion,
    Reseau,
    contactid,
    type_contact_corrige
    )a2
where type_contact_corrige != 'NULL'















/*extract1*/
drop table if exists etudes.fer_extract1_part1;
create table etudes.fer_extract1_part1 as 
select * from 
(select 
mcc,
cASE
        WHEN ert IN ("20","21","22","24","26","27","28","64","B1") THEN "VAD"
        WHEN ert IN("00","10","30","40","41","42","43","45","46","47","48","49","50","52","54","57","60","65","75","80","92") THEN "PROXI"
    END AS famille_ERT,
sum(mt_brut_fact) as mt_brut_fact,
sum(nb_facture) as nb_facture,
dept_n3,
contactid,
to_timestamp(concat("20", substring(aamm, 1, 2), "/", substring(aamm, 3, 2), "/01"), "yyyy/MM/dd") as aamm
from pdv.dam01_view 
group by aamm,famille_ERT,dept_n3,contactid,mcc)t1
where aamm>=date_sub(now(), interval 2 years)

 drop table if exists etudes.fer_extract1_part2;
 create table etudes.fer_extract1_part2 as 
 select a.*,b.famille_mc
 from etudes.fer_extract1_part1 a 
 inner join marketing.Famille_code_MCC_vfinal_2019 b 
 on a.mcc=b.mcc ;

drop table if exists etudes.fer_extract_part3;
create table etudes.fer_extract1 as 
select 
concat("20",a.aamm) as mois,
sum(a.mt_brut_fact) as valeur,
sum(a.nb_facture) as nombre,
a.famille_mc as famille_mcc,
b.region_de_france,
case 
        when a.famille_ERT="PROXI" and a.contactid="0" then "PROXI_CONTACT"
        when a.famille_ERT="PROXI" and a.contactid="1" then "PROXI_SANSCONTACT"
        when a.famille_ERT="PROXI" and a.contactid="2" then "PROXI_SANSCONTACT"
        when a.famille_ERT="PROXI" and a.contactid='3' then 'PROXI_CONTACT'
        when a.famille_ERT="VAD"   and a.contactid="0" then 'VAD_CONTACT'
        when a.famille_ERT="VAD"   and a.contactid="1" then 'VAD_CONTACT'
        when a.famille_ERT="VAD"   and a.contactid='2' then 'VAD_CONTACT'
        when a.famille_ERT="VAD"   and a.contactid='3' then 'VAD_CONTACT'
        when a.famille_ERT="NULL"   and a.contactid="0" then 'NULL'
        when a.famille_ERT="NULL"   and a.contactid="1" then 'NULL'
        when a.famille_ERT="NULL"   and a.contactid='2' then 'NULL'
        when a.famille_ERT="NULL"   and a.contactid='3' then 'NULL'
end as type_de_paiement
from etudes.fer_extract1_part2 a 
inner join 
default.fer_ref_regionfrance b
on a.dept_n3=b.depid
group by a.aamm,famille_mcc,b.region_de_france,type_de_paiement;

create table etudes.fer_extract1 select * from etudes.fer_extract_part3
where type_de_paiement is not null;



/*extract 2*/
create table etudes.fer_extract2_part1
select 
concat("20",a.aamm) as mois,
sum(a.mt_brut_fact) as valeur,
sum(a.nb_facture) as nombre,
a.famille_mc as famille_mcc,
a.mcc,
case 
        when a.famille_ERT="PROXI" and a.contactid="0" then "PROXI_CONTACT"
        when a.famille_ERT="PROXI" and a.contactid="1" then "PROXI_SANSCONTACT"
        when a.famille_ERT="PROXI" and a.contactid="2" then "PROXI_SANSCONTACT"
        when a.famille_ERT="PROXI" and a.contactid='3' then 'PROXI_CONTACT'
        when a.famille_ERT="VAD"   and a.contactid="0" then 'VAD_CONTACT'
        when a.famille_ERT="VAD"   and a.contactid="1" then 'VAD_CONTACT'
        when a.famille_ERT="VAD"   and a.contactid='2' then 'VAD_CONTACT'
        when a.famille_ERT="VAD"   and a.contactid='3' then 'VAD_CONTACT'
        when a.famille_ERT="NULL"   and a.contactid="0" then 'NULL'
        when a.famille_ERT="NULL"   and a.contactid="1" then 'NULL'
        when a.famille_ERT="NULL"   and a.contactid='2' then 'NULL'
        when a.famille_ERT="NULL"   and a.contactid='3' then 'NULL'
end as type_de_paiement
from etudes.fer_extract1_part2 a 
group by a.aamm,famille_mcc,mcc,type_de_paiement;

create table etudes.fer_extract2 as select * from etudes.fer_extract2_part1
where type_de_paiement is not null ;


/*extract 3*/

create table etudes.fer_test as
select e_nu_ano_numero_anonymise, sum(b09_montant_brut_de_la_transaction/100) as mnt,
        count(b09_montant_brut_de_la_transaction) as
       datediff(max(b05_date_locale_transaction_timestamp),min(b05_date_locale_transaction_timestamp)) as nbJours
from   x_compensation.transactions
 where year(b05_date_locale_transaction_timestamp)=year(current_timestamp())-1
group  by e_nu_ano_numero_anonymise;

create table etudes.fer_test2 as
select *,
       mnt * 365 / nbJours as mntEstim
from etudes.fer_test
where nbJours > 60;

create table etudes.fer_test3 as
select *,
       floor((mntEstim-0.01)/42)*42 as seuilBas,
       ceil((mntEstim-0.01)/42)*42 as seuilHaut
from etudes.fer_test2;

create table etudes.fer_cartes as
select e_nu_ano_numero_anonymise,
       b15_code_activite_de_laccepteur___code_mcc,
       sum(b09_montant_brut_de_la_transaction/100) as montant
from   x_compensation.transactions
where  substr(b05_date_locale_transaction,1,2)="18"
group by e_nu_ano_numero_anonymise, b15_code_activite_de_laccepteur___code_mcc;

create table etudes.fer_cartes2 as
select a.*, b.seuilBas, b.seuilHaut,b.nombre 
from   etudes.fer_cartes a
inner join etudes.fer_test3 b 
on a.e_nu_ano_numero_anonymise = b.e_nu_ano_numero_anonymise;

create table etudes.fer_cartes3 as
select seuilBas, seuilHaut,
       b15_code_activite_de_laccepteur___code_mcc,
       sum(montant) as montantTot
from   etudes.fer_cartes2
group by seuilBas, seuilHaut, b15_code_activite_de_laccepteur___code_mcc;


 
drop table etudes.fer_cartesFin
create table etudes.fer_cartesFin as
select a.b15_code_activite_de_laccepteur___code_mcc as mcc,a.valeur as valeur,a.nombre, b.famille_mc as famille_mcc,concat("[",cast(a.seuilbas as string),"-",cast(a.seuilhaut as string),"]") as segment
from   etudes.fer_cartes3 a
inner join marketing.Famille_code_MCC_vfinal_2019_1 b
on a.b15_code_activite_de_laccepteur___code_mcc = b.mcc;















































SELECT nb_transactions,b20_numero_de_departement FROM
(SELECT 
b20_numero_de_departement,
COUNT(b09_montant_brut_de_la_transaction/100) AS nb_transactions
FROM x_compensation.transactions
WHERE annee_reception_banque=2018
GROUP BY b20_numero_de_departement)a1
ORDER by nb_transactions DESC;

        /***** on recherche les valeurs manquantes dans la variable  dept *****/
SELECT DISTINCT(LENGTH(b20_numero_de_departement)) FROM x_compensation.transactions
SELECT COUNT(b20_numero_de_departement) 
FROM x_compensation.transactions
WHERE transactions.b20_numero_de_departement='   '
AND transactions.annee_reception_banque=2018
-- on a 106 843 656 valeurs manquantes sur 4 745 773 989. Soit 2.25% de valeurs manquantes
;

    /*On compte le nombre de cartes par département²

SELECT COUNT(e_nu_ano_numero_anonymise),b20_numero_de_departement
FROM 
    (   
    SELECT  e_nu_ano_numero_anonymise,
        b20_numero_de_departement
        FROM
        
        (SELECT t1.*, row_number() over (PARTITION BY b20_numero_de_departement ORDER BY NB_TRANSACTIONS DESC) AS seqnum
        FROM
        
            (SELECT
                e_nu_ano_numero_anonymise,
                b20_numero_de_departement,
                COUNT(b09_montant_brut_de_la_transaction) as NB_TRANSACTIONS
            FROM x_compensation.transactions
            WHERE annee_reception_banque=2018
            GROUP BY b20_numero_de_departement,e_nu_ano_numero_anonymise
            ) t1
        
        ) a
        WHERE seqnum=1
        
    )a1
GROUP BY b20_numero_de_departement

*/
--MARCHE PAS

---
---On crée cette table pour compter le nombre de transactions par porteur et par département
-- on compte le nombre de transactions par carte 
CREATE TABLE DEFAULT.TEST1 AS
select
                    NB_TRANSACTIONS,
                    b20_numero_de_departement,
                    Nb_Cartes
        FROM(  SELECT
                    e_nu_ano_numero_anonymise as Nb_Cartes,
                    b20_numero_de_departement,
                    COUNT(b09_montant_brut_de_la_transaction) as NB_TRANSACTIONS
                FROM x_compensation.transactions
                WHERE annee_reception_banque=2018
                GROUP BY b20_numero_de_departement,e_nu_ano_numero_anonymise
            )a1
   ;

 --



-- voici la formule sur impala pour selectionner une ligne particulière d'une table en fonction
-- de la valeur maximale d'une colonne. Ici, on voulait le numéro de département qui avait le 
-- plus grand nombre de transactions


                            )
-- Cette formule nous permet de trouver le nombre de cartes par departement, en partant du maximum de nombre de transactions par carte porteur pour assigné un departement de résidence à un porteur de carte, puiis
--ensuite on compte tranquillement le nombre de cartes par département
SELECT count(nb_cartes),b20_numero_de_departement FROM 
(       SELECT groupedtt.nb_cartes,MAXNBTRANS,b20_numero_de_departement
        FROM DEFAULT.TEST1 tt
        INNER JOIN 
            ( SELECT  MAX(NB_TRANSACTIONS) AS MAXNBTRANS,nb_cartes
                FROM DEFAULT.TEST1 
                GROUP BY nb_cartes  
            ) groupedtt 
        ON tt.nb_cartes = groupedtt.nb_cartes
        AND tt.NB_TRANSACTIONS=groupedtt.MAXNBTRANS
) a7
GROUP BY b20_numero_de_departement          
























create table default.fer_pmu_102 as 
select count(distinct(b14_siret)) from
(select 
    sum(b09_montant_brut_de_la_transaction/100) as b09_montant_brut_de_la_transaction,
    count(b09_montant_brut_de_la_transaction) as nb_transactions,
    e_mbr_acq_membre_principal_acquereur,
    e_mbr_emet_membre_principal_emetteur,
    b05_date_locale_transaction,
    b14_siret
from 
    (select * from x_compensation.transactions 
    inner join 
    default.fer_regroupement_siret_pmu
    on b14_siret=siret
    where s04_code_operation=100
    )a1
--where b17_libelle_enseigne_commerciale like ("%PMU")
/*and*/ where  b05_date_locale_transaction like ("18%")
group by    e_mbr_acq_membre_principal_acquereur ,
            e_mbr_emet_membre_principal_emetteur,
            b05_date_locale_transaction,
            b14_siret
            )a2


;

select b17_libelle_enseigne_commerciale from x_compensation.transactions
where b14_siret in ("77567125800017", "77567125802757")
;
;

select distinct(b08_environnement_reglementaire__technique_de_la_transaction) from x_compensation.transactions 
where b17_libelle_enseigne_commerciale like ("%PMU%") annee_reception_banque annee_reception_banque 
;

/*operation credit pmu all time ; 4 Sirets*/
select distinct(b14_siret) as siret from x_compensation.transactions
where b17_libelle_enseigne_commerciale like ("%PMU%")
and s04_code_operation=102;

/*operation credit pmu 2018 ; 2 Sirets*/
select 
     distinct(b14_siret) as siret
from x_compensation.transactions
where b17_libelle_enseigne_commerciale like ("%PMU%")
and annee_reception_banque=2018
and s04_code_operation=102;

/*operation=100 and pmu 2018 ; 64 Sirets*/
select distinct(b14_siret) as siret from x_compensation.transactions
where b17_libelle_enseigne_commerciale like ("%PMU%")
and annee_reception_banque=2018
and s04_code_operation=100;

/*operation all code operation ; 81 Sirets*/
select distinct(b14_siret) as siret from x_compensation.transactions
where b17_libelle_enseigne_commerciale like ("%PMU%");


/*Compte*/
create table default.fer_pmu_102 as 
select 
    sum(b09_montant_brut_de_la_transaction/100) as b09_montant_brut_de_la_transaction,
    count(b09_montant_brut_de_la_transaction) as nb_transactions,
    e_mbr_acq_membre_principal_acquereur,
    e_mbr_emet_membre_principal_emetteur,
    b05_date_locale_transaction,
    b14_siret,
    b08_environnement_reglementaire__technique_de_la_transaction
from x_compensation.transactions
where b17_libelle_enseigne_commerciale like ("%PMU%")
and annee_reception_banque b05_date_locale_transaction like ("18%")
and s04_code_operation=102
group by  
    e_mbr_acq_membre_principal_acquereur,
    e_mbr_emet_membre_principal_emetteur,
    b05_date_locale_transaction,
    b14_siret,
    b08_environnement_reglementaire__technique_de_la_transaction
    

create table default.fer_pmu_100 as 
select 
    sum(b09_montant_brut_de_la_transaction/100) as b09_montant_brut_de_la_transaction,
    count(b09_montant_brut_de_la_transaction) as nb_transactions,
    e_mbr_acq_membre_principal_acquereur,
    e_mbr_emet_membre_principal_emetteur,
    b05_date_locale_transaction,
    b14_siret,
    b08_environnement_reglementaire__technique_de_la_transaction
from x_compensation.transactions
where b17_libelle_enseigne_commerciale like ("%PMU%")
and b05_date_locale_transaction like ("18%")
and s04_code_operation=100
group by  
    e_mbr_acq_membre_principal_acquereur,
    e_mbr_emet_membre_principal_emetteur,
    b05_date_locale_transaction,
    b14_siret,
    b08_environnement_reglementaire__technique_de_la_transaction
























    create table DEFAULT.TEST
as select mt_brut_fact,nb_facture,aamm,ref_acq,mcc,srt
from PDV.DAM01_VIEW 
WHERE aamm="1712"
group by mt_brut_fact,nb_facture,aamm,ref_acq,mcc,srt;

create table default.test1
as select *
FROM default.test 
INNER JOIN 
default.mbracq_s06
ON ref_acq=trim(s06_identifiant_etablissement_donneur_dordre);


/*creation de la table recesant la dam du 1217 pour LCL 30002*/
create table default.damLCL1712 as 
select sum(mt_brut_fact) as ca, sum(nb_facture) as nb_transa,aamm,ref_acq,mcc,srt,e_mbr_acq_membre_principal_acquereur,s06_identifiant_etablissement_donneur_dordre 
FROM default.test1 
where e_mbr_acq_membre_principal_acquereur="30002"
group by aamm,ref_acq,mcc,srt,e_mbr_acq_membre_principal_acquereur,s06_identifiant_etablissement_donneur_dordre;

/*on compte le nombre d'observations, on en a 86724*/
select count(nb_transa)from default.damlcl1712



/*Compens*/
create table default.transaLCL1712 as 
select b15_code_activite_de_laccepteur___code_mcc,b14_siret,s04_code_operation,sum(b09_montant_brut_de_la_transaction/100) as ca ,count(b09_montant_brut_de_la_transaction) nbtr_trans,annee_reception_banque,mois_reception_banque,e_mbr_acq_membre_principal_acquereur,s06_identifiant_etablissement_donneur_dordre 
from x_compensation.transactions
where e_mbr_acq_membre_principal_acquereur="30002"
and annee_reception_banque=2017
and mois_reception_banque=12
group by b15_code_activite_de_laccepteur___code_mcc,b14_siret,s04_code_operation,e_mbr_acq_membre_principal_acquereur,s06_identifiant_etablissement_donneur_dordre,annee_reception_banque,mois_reception_banque;







/* Ne marche pas non plus faire un full join*/
create table default.fullejoin1
as select b14_siret,nbtr_trans  from default.transalcl1712
INNER JOIN 
default.damlcl1712
ON srt=b14_siret;

select * from default.fullejoin1;

/*INUTILE table difference: on veut obtenir la différence entre 2 colonnes de 2 différentes tables 
cad la différence entre le nombre de transaction dans DAM et le nombre de transaction dans Compensation(cv table transactions)*/

select 
T1.TOTALNBTR - T2.TOTALNB as balanceonhand
from
(select NBTR_TRANS as TOTALNBTR 
    from default.transalcl1712 
    ) T1, 
(select nb_transa as TOTALNB 
    from default.damlcl1712 
    ) T2 
;
-- mais ce programme ne crée qu'une colonne balanceonhand
    donc insuffisant;
    
    
    
    
/***Programme *****/
create table default.diff_transa_compDAM as 
select T1.b14_siret as siret, T1.nbtr_trans as nb_tran_incompensation,
(T1.nbtr_trans-t2.nb_transa) as Diff,t2.nb_transa as nb_tran_inDAM
FROM default.transalcl1712 as T1 
INNER JOIN
default.damlcl1712 as T2
ON T1.b14_siret=T2.srt;
-- et puis si on voulait la différence absolue on aurait mis abs devant (T1.nbtr_trans-t2.nb_transa) 
create table default.diff_transa_compDAM1 as 
select * from default.diff_transa_compDAM
where diff>0
;
/*cette requete fonctionne pas car la jointure on srt biaise le resultat qu'on veut obtenir et ne se
concentre pas sur le nombtre de transactions donc pleins de transactions ne sont pas pris en compte
si le siret est le meme pour les 2 tables*/
create table default.Comp_no_in_dam_lcl1712
as select
b14_siret,e_mbr_acq_membre_principal_acquereur,S06_identifiant_etablissement_donneur_dordre,ca,nbtr_trans
FROM default.transalcl1712
LEFT ANTI JOIN 
default.damlcl1712
ON srt=b14_siret;

select sum(nbtr_trans) from default.Comp_no_in_dam_lcl1712
/* Maintenant que nous avons nos 2 tables, default.diff_transa_compDAM et default.test3*/

select b14_siret,e_mbr_acq_membre_principal_acquereur,nbtr_trans from default.diff_transa_compDAM1
union
default.Comp_no_in_dam_lcl1712
on b14_siret=siret;


create table default.diff_transa_comp_dam
as select
siret,diff as nb_transaction
from default.diff_transa_compdam1
alter table default.joint2t rename to default.diff_transa_comp_dam

create table default.noindaam
as select 
b14_siret as siret,
nbtr_trans,
e_mbr_acq_membre_principal_acquereur
from default.comp_no_in_dam_lcl1712

create table default.nbtransa_no_in_dam_lcl1712
as select * from default.diff_transa_comp_dam
UNION 
select * from default.noindaam

select sum(nb_transaction) from default.diff_transa_comp_dam
select sum(nbtr_trans) from default.noindaam
select sum(nb_transaction) from default.nbtransa_no_in_dam_lcl1712


































/*Première table : Vérification de la volumétrie du nombre de sirets pour chaque couple partenaire mbr_acq/prssi par rapport au moins précédent
   Pour la réalisation de cette requête, nous allons réaliser plusieurs étapes: 
   -créer le couple partenaire mbr acq et prssi
   -creation d'une variable pourcentage pour compter l'écart de contrat entre le mois d'aout et de Septembre

 */
select * from
(select 
a2.prssi,
a2.mbr_acq,
aout2018,
septembre2018,
(1-(aout2018/septembre2018))*100 as pourcentage

from 
(select 
    prssi,
    mbr_acq,
    count(distinct(srt)) as aout2018
from pdv.dam01_view 
where aamm in ('1808')
group by 
    prssi, 
    mbr_acq)a2
inner join 
(select 
    prssi,
    mbr_acq,
    count(distinct(srt) ) as septembre2018
from pdv.dam01_view 
where aamm in ('1809')
group by 
    prssi, 
    mbr_acq )a1
on a2.prssi=a1.prssi and a2.mbr_acq=a1.mbr_acq)a3
where pourcentage>10
;





/*Deuxieme table : Verification de la volumétrie des contrats 'ID_PDV_BQE' par SIRET'SRT' par rapport au moins precedent
   Pour la réalisation de cette requête, nous allons réaliser plusieurs étapes: 
   -créer le couple partenaire mbr acq et prssi
   -creation d'une variable pourcentage pour compter l'écart de contrat entre le mois d'aout et de Septembre

 */
select * from 
(select 
    a1.prssi,
    a1.mbr_acq,
    a1.srt,
    a1.nombre_contrat_par_siret_septembre,
    a2.nombre_contrat_par_siret_aout,
    (1-(a2.nombre_contrat_par_siret_aout/a1.nombre_contrat_par_siret_septembre))*100 as pourcentage
from 
(select 
    prssi,
    mbr_acq, 
    srt, 
    count(distinct(id_pdv_bqe)) as nombre_contrat_par_siret_septembre, 
    aamm
from pdv.dam01_view
where aamm in ('1809') 
group by 
prssi, 
mbr_acq,
srt,
aamm)a1

full join  

(select 
    prssi,
    mbr_acq, 
    srt, 
    count(distinct(id_pdv_bqe)) as nombre_contrat_par_siret_aout, 
    aamm
from pdv.dam01_view
where aamm in ('1808') 
group by 
prssi, 
mbr_acq,
srt,
aamm)a2
on a1.prssi=a2.prssi and a1.mbr_acq=a2.mbr_acq and a1.srt=a2.srt)a3
/*where pourcentage>10*/



































Create table default.fer_requete1_1 as 
/* select 
    *,
    case
        when bqe_pay != "FR" THEN "99"
        else "FR"
    End as "departement"
FROM
(*/ SELECT 
    sum_brut_soustraction,
    aamm,
    lib_contact_id_corrige,
    ert,
    srt,
    nbfact_DAM,
    ref_acq,
    res_id,
	Reseau,
	mbr_acq
/*	bqe_rsn,
	bqe_esg,
	bqe_lct,
	bqe_pay,
	bqe_naf_val_old,
	bqe_naf_val_new,
	bqe_mcc,
	bqe_dep_n3 */

FROM 
(SELECT 
    sum_brut_soustraction,
    aamm,
    lib_contact_id_corrige,
    ert,
    srt,
    nbfact_DAM,
    ref_acq,
    res_id,
	Reseau,
	mbr_acq,
    to_timestamp(concat("20", substring(aamm, 1, 2), "/",  substring(aamm, 3, 2), "/01"), "yyyy/MM/dd") as aamm1,
	concat( "01","/",  substring(aamm, 3, 2),"/","20", substring(aamm, 1, 2)) as dam_date
FROM (
        SELECT

            sum(mt_brut_soustraction) as sum_brut_soustraction,
            aamm ,
            ert,
            familleert,
            ref_acq,
            sum(nb_facture) AS nbfact_DAM,
            srt,
            contactid,
            res_id,
			Reseau,
			mbr_acq
        FROM
            (
                SELECT 
                    ((mt_brut_fact)-(mt_annul_fact)) as mt_brut_soustraction,
                    aamm,
                    ert,
                    mbr_acq,
					ref_acq,
                    srt,
                    contactid,
                    nb_facture,
                    res_id,
					CASE
						WHEN ert IN ("20","21","22","24","26","27","28","64","B1") THEN "VAD"
						WHEN ert IN("00","10","30","40","41","42","43","45","46","47","48","49","50","52","54","57","60","65","75","80","92") THEN "PROXI"
						else "autre"
					END AS familleert,
					CASE 
						WHEN res_id="1" then "CB"
						WHEN res_id="2" then "Visa"
						WHEN res_id="3" then "MasterCard"
					END AS Reseau
				FROM pdv.dam01_view 
                
                
            )a1
            GROUP BY aamm,ert,familleert,ref_acq,srt,contactid,res_id,Reseau,mbr_acq
     ) a2
    
            INNER JOIN 
            DEFAULT.fer_type_contact_corrige AS t3
                ON a2.contactid=t3.contact_id AND a2.familleert=t3.famille_ert
    )a3
/*    INNER JOIN
pdv.ppdv01
ON mbr_acq=bqe_mbr_acq*/
			where aamm1 >=date_sub(now(),INTERVAL 2 YEAR)
/*) a4*/
;
			










select 
sum(mt_brut_fact)
from
(select 
    mt_brut_fact,
    srt,
    ref_acq
from pdv.dam01_view
where ert IN ("00","10","30","40","41","42","43","45","46","47","48","49","50","52","54","57","60","65","75","80","92") 
    AND res_id='1'
    and aamm='1810'
    and contactid='2')a1
inner join 
   (select 
        e_mbr_acq_membre_principal_acquereur,
        b14_siret,
        (b09_montant_brut_de_la_transaction/100) as montant_transaction
        from x_compensation.transactions 
        where b05_date_locale_transaction like '1810%'
        and b08_environnement_reglementaire__technique_de_la_transaction in  ("00","10","30","40","41","42","43","45","46","47","48","49","50","52","54","57","60","65","75","80","92") )a2
    on a1.srt=a2.b14_siret and a1.ref_acq=a2.e_mbr_acq_membre_principal_acquereur and a1.mt_brut_fact=a2.montant_transaction;
-- 1 847 321 transactions
-- montant de 3 946 992 euros 

select 
    count(mt_brut_fact)
from pdv.dam01_view
where ert IN ("00","10","30","40","41","42","43","45","46","47","48","49","50","52","54","57","60","65","75","80","92") 
    AND res_id='1'
    and aamm='1810'
    and contactid='2';
    



select 
   *
from x_compensation.transactions
where b05_date_locale_transaction like '1810%'




