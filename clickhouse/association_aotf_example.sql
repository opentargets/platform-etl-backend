select groupArray(datatype_id) from (
SELECT datatype_id
FROM ot.associations_otf_left l
group by datatype_id
order by datatype_id);


SELECT datatype_id,
       groupUniqArray(datasource_id) as datasource_ids
FROM ot.associations_otf_left l
group by datatype_id
order by datatype_id;

select distinct datatype_id, datasource_id from ot.associations_otf_left where datasource_id not in NULL;

SELECT datasource_id,
       any(datatype_id) as datatype_id
FROM ot.associations_otf_left l
group by datasource_id;

select * from ot.associations_otf_right limit 1;

-- fixing target
WITH
    groupArray((score_per_datasource, datasource_weight, datasource_id, datatype_id)) AS score_datasource_v,
    arrayReverseSort(a -> (a.1), arrayMap((i, j) -> ((i.1 * i.2) / pow(j, 2), i.3, i.4), score_datasource_v,
                                          range(1, length(score_datasource_v) + 1))) AS score_datasource_weighted_sorted_v,
        arraySum(score_datasource_weighted_sorted_v.1) / 1.66 AS score_overall,
        arrayMap(x -> (x, arraySum(arrayFilter(a -> a.3 = x, score_datasource_weighted_sorted_v).1) / 1.66),
                groupUniqArray(datatype_id)) as score_datatype
SELECT
    B,
    score_overall
--     score_datatype,
--     score_datasource_weighted_sorted_v as score_datasource
FROM
    (
     WITH
             arraySum(arrayMap((x, y) -> (x / pow(y, 2)),
                               arrayReverseSort(groupArray(row_score)),
                               range(1, length(groupArray(row_score)) + 1))) / 1.66 AS score_per_datasource,
         any(B_data) as right_data,
         ifNull(any(weight), 1.0) as datasource_weight

     SELECT
         B,
         any(datatype_id) as datatype_id,
         datasource_weight,
         datasource_id,
         score_per_datasource

     FROM ot.associations_otf_right l
              LEFT OUTER JOIN (
         WITH
             arrayJoin([('sysbio', 0.5), ('slapenrich', 0.5), ('progeny', 0.5), ('europepmc', 0.2), ('expression_atlas', 0.2), ('phenodigm', 0.2)] AS src) AS dst
         SELECT dst.1 as datasource_id,
                toNullable(dst.2) as weight
         ORDER BY datasource_id) r USING (datasource_id)
         PREWHERE ((A IN (
         WITH arrayJoin(['ENSG00000072210']) as As
         SELECT As
     )) AND datasource_id <> 'expression_atlas')
         OR (A = 'ENSG00000072210' and datasource_id = 'expression_atlas')
     GROUP BY
         B,
         datasource_id
--    HAVING (B_data LIKE '%BRCA%')
        )
GROUP BY B
HAVING score_overall > 0
ORDER BY score_overall DESC;


-- fixing disease
WITH
    groupArray((score_per_datasource, datasource_weight, datasource_id, datatype_id)) AS score_datasource_v,
    arrayReverseSort(a -> (a.1), arrayMap((i, j) -> ((i.1 * i.2) / pow(j, 2), i.3, i.4), score_datasource_v,
                                          range(1, length(score_datasource_v) + 1))) AS score_datasource_weighted_sorted_v,
    arraySum(score_datasource_weighted_sorted_v.1) / 1.66 AS score_overall,
     arrayMap(x -> (x, arraySum(arrayFilter(a -> a.3 = x, score_datasource_weighted_sorted_v).1) / 1.66),
              groupUniqArray(datatype_id)) as score_datatype
SELECT
    B,
    score_overall,
    score_datatype,
    score_datasource_weighted_sorted_v as score_datasource
FROM
    (
     WITH
        arraySum(arrayMap((x, y) -> (x / pow(y, 2)),
            arrayReverseSort(groupArray(row_score)),
            range(1, length(groupArray(row_score)) + 1))) / 1.66 AS score_per_datasource,
          any(B_data) as B_data,
         ifNull(any(weight), 1.0) as datasource_weight

     SELECT
        B,
        any(datatype_id) as datatype_id,
        datasource_weight,
        datasource_id,
        score_per_datasource

     FROM ot.associations_otf_left l
     LEFT OUTER JOIN (
         WITH
            arrayJoin([('sysbio', 0.5), ('slapenrich', 0.5), ('progeny', 0.5), ('europepmc', 0.2), ('expression_atlas', 0.2), ('phenodigm', 0.2)] AS src) AS dst
         SELECT dst.1 as datasource_id,
                toNullable(dst.2) as weight
         ORDER BY datasource_id) r USING (datasource_id)
    PREWHERE ((A IN (
            WITH arrayJoin(['EFO_0000616', 'EFO_0000094','EFO_0000095','EFO_0000096','EFO_0000178','EFO_0000181','EFO_0000182','EFO_0000183','EFO_0000186','EFO_0000191','EFO_0000197','EFO_0000198','EFO_0000199','EFO_0000200','EFO_0000209','EFO_0000216','EFO_0000218','EFO_0000220','EFO_0000221','EFO_0000222','EFO_0000228','EFO_0000231','EFO_0000232','EFO_0000239','EFO_0000272','EFO_0000292','EFO_0000294','EFO_0000304','EFO_0000305','EFO_0000308','EFO_0000309','EFO_0000311','EFO_0000313','EFO_0000318','EFO_0000319','EFO_0000326','EFO_0000333','EFO_0000339','EFO_0000348','EFO_0000349','EFO_0000364','EFO_0000365','EFO_0000384','EFO_0000389','EFO_0000402','EFO_0000403','EFO_0000437','EFO_0000466','EFO_0000474','EFO_0000478','EFO_0000489','EFO_0000497','EFO_0000499','EFO_0000501','EFO_0000503','EFO_0000508','EFO_0000512','EFO_0000514','EFO_0000516','EFO_0000519','EFO_0000536','EFO_0000537','EFO_0000538','EFO_0000540','EFO_0000545','EFO_0000549','EFO_0000558','EFO_0000563','EFO_0000564','EFO_0000565','EFO_0000569','EFO_0000571','EFO_0000574','EFO_0000588','EFO_0000589','EFO_0000616','EFO_0000618','EFO_0000621','EFO_0000622','EFO_0000625','EFO_0000630','EFO_0000632','EFO_0000637','EFO_0000639','EFO_0000640','EFO_0000641','EFO_0000651','EFO_0000653','EFO_0000662','EFO_0000673','EFO_0000677','EFO_0000681','EFO_0000684','EFO_0000691','EFO_0000693','EFO_0000698','EFO_0000701','EFO_0000702','EFO_0000705','EFO_0000707','EFO_0000708','EFO_0000731','EFO_0000756','EFO_0000759','EFO_0000760','EFO_0000763','EFO_0000764','EFO_0000768','EFO_0000770','EFO_0000771','EFO_0001059','EFO_0001061','EFO_0001069','EFO_0001071','EFO_0001073','EFO_0001075','EFO_0001376','EFO_0001378','EFO_0001379','EFO_0001380','EFO_0001416','EFO_0001421','EFO_0001422','EFO_0001642','EFO_0001663','EFO_0002087','EFO_0002422','EFO_0002424','EFO_0002425','EFO_0002426','EFO_0002427','EFO_0002428','EFO_0002431','EFO_0002461','EFO_0002517','EFO_0002618','EFO_0002621','EFO_0002625','EFO_0002626','EFO_0002627','EFO_0002630','EFO_0002890','EFO_0002892','EFO_0002893','EFO_0002913','EFO_0002914','EFO_0002916','EFO_0002917','EFO_0002918','EFO_0002919','EFO_0002920','EFO_0002921','EFO_0002938','EFO_0002939','EFO_0002945','EFO_0002970','EFO_0003014','EFO_0003025','EFO_0003028','EFO_0003032','EFO_0003050','EFO_0003060','EFO_0003086','EFO_0003099','EFO_0003100','EFO_0003101','EFO_0003104','EFO_0003106','EFO_0003767','EFO_0003769','EFO_0003777','EFO_0003817','EFO_0003818','EFO_0003820','EFO_0003824','EFO_0003825','EFO_0003826','EFO_0003828','EFO_0003832','EFO_0003833','EFO_0003835','EFO_0003839','EFO_0003841','EFO_0003844','EFO_0003850','EFO_0003851','EFO_0003853','EFO_0003859','EFO_0003860','EFO_0003863','EFO_0003865','EFO_0003866','EFO_0003868','EFO_0003869','EFO_0003871','EFO_0003872','EFO_0003873','EFO_0003875','EFO_0003880','EFO_0003891','EFO_0003893','EFO_0003897','EFO_0003899','EFO_0003966','EFO_0003967','EFO_0003968','EFO_0004142','EFO_0004149','EFO_0004193','EFO_0004198','EFO_0004199','EFO_0004230','EFO_0004238','EFO_0004243','EFO_0004244','EFO_0004248','EFO_0004260','EFO_0004264','EFO_0004267','EFO_0004280','EFO_0004281','EFO_0004288','EFO_0004289','EFO_0004606','EFO_0004986','EFO_0005088','EFO_0005140','EFO_0005220','EFO_0005221','EFO_0005232','EFO_0005235','EFO_0005406','EFO_0005537','EFO_0005539','EFO_0005540','EFO_0005541','EFO_0005543','EFO_0005548','EFO_0005553','EFO_0005561','EFO_0005570','EFO_0005577','EFO_0005578','EFO_0005582','EFO_0005584','EFO_0005588','EFO_0005591','EFO_0005592','EFO_0005629','EFO_0005631','EFO_0005701','EFO_0005708','EFO_0005716','EFO_0005741','EFO_0005753','EFO_0005754','EFO_0005755','EFO_0005769','EFO_0005771','EFO_0005772','EFO_0005774','EFO_0005784','EFO_0005785','EFO_0005802','EFO_0005803','EFO_0005804','EFO_0005842','EFO_0005922','EFO_0005950','EFO_0005952','EFO_0006318','EFO_0006387','EFO_0006452','EFO_0006460','EFO_0006462','EFO_0006463','EFO_0006497','EFO_0006500','EFO_0006544','EFO_0006545','EFO_0006772','EFO_0006858','EFO_0006859','EFO_0006890','EFO_0007133','EFO_0007134','EFO_0007176','EFO_0007201','EFO_0007206','EFO_0007252','EFO_0007271','EFO_0007321','EFO_0007330','EFO_0007331','EFO_0007333','EFO_0007352','EFO_0007355','EFO_0007360','EFO_0007362','EFO_0007365','EFO_0007373','EFO_0007378','EFO_0007384','EFO_0007392','EFO_0007408','EFO_0007412','EFO_0007416','EFO_0007422','EFO_0007441','EFO_0007458','EFO_0007466','EFO_0007491','EFO_0007532','EFO_0008492','EFO_0008498','EFO_0008499','EFO_0008500','EFO_0008505','EFO_0008506','EFO_0008509','EFO_0008514','EFO_0008524','EFO_0008528','EFO_0008549','EFO_0008550','EFO_0008560','EFO_0008573','EFO_0008581','EFO_0008615','EFO_0008622','EFO_0008624','EFO_0009000','EFO_0009001','EFO_0009002','EFO_0009119','EFO_0009255','EFO_0009259','EFO_0009260','EFO_0009314','EFO_0009385','EFO_0009386','EFO_0009387','EFO_0009431','EFO_0009433','EFO_0009448','EFO_0009452','EFO_0009455','EFO_0009464','EFO_0009468','EFO_0009469','EFO_0009481','EFO_0009483','EFO_0009484','EFO_0009488','EFO_0009528','EFO_0009532','EFO_0009534','EFO_0009541','EFO_0009542','EFO_0009544','EFO_0009546','EFO_0009547','EFO_0009548','EFO_0009549','EFO_0009555','EFO_0009556','EFO_0009601','EFO_0009602','EFO_0009605','EFO_0009607','EFO_0009608','EFO_0009619','EFO_0009660','EFO_0009662','EFO_0009663','EFO_0009670','EFO_0009672','EFO_0009673','EFO_0009674','EFO_0009675','EFO_0009676','EFO_0009682','EFO_0009685','EFO_0009689','EFO_0009690','EFO_0009709','EFO_0009781','EFO_0009812','EFO_0010176','EFO_0010282','EFO_0010283','EFO_0010284','EFO_0010285','EFO_1000017','EFO_1000018','EFO_1000020','EFO_1000021','EFO_1000027','EFO_1000028','EFO_1000040','EFO_1000042','EFO_1000044','EFO_1000045','EFO_1000049','EFO_1000051','EFO_1000052','EFO_1000053','EFO_1000068','EFO_1000070','EFO_1000073','EFO_1000079','EFO_1000088','EFO_1000089','EFO_1000103','EFO_1000105','EFO_1000110','EFO_1000121','EFO_1000122','EFO_1000124','EFO_1000125','EFO_1000130','EFO_1000142','EFO_1000143','EFO_1000151','EFO_1000154','EFO_1000155','EFO_1000157','EFO_1000158','EFO_1000172','EFO_1000188','EFO_1000193','EFO_1000194','EFO_1000195','EFO_1000197','EFO_1000198','EFO_1000200','EFO_1000203','EFO_1000210','EFO_1000217','EFO_1000218','EFO_1000219','EFO_1000223','EFO_1000233','EFO_1000240','EFO_1000248','EFO_1000251','EFO_1000255','EFO_1000267','EFO_1000268','EFO_1000271','EFO_1000278','EFO_1000280','EFO_1000282','EFO_1000284','EFO_1000287','EFO_1000288','EFO_1000289','EFO_1000297','EFO_1000304','EFO_1000307','EFO_1000310','EFO_1000328','EFO_1000336','EFO_1000344','EFO_1000345','EFO_1000346','EFO_1000352','EFO_1000355','EFO_1000356','EFO_1000359','EFO_1000362','EFO_1000363','EFO_1000366','EFO_1000379','EFO_1000382','EFO_1000384','EFO_1000388','EFO_1000393','EFO_1000397','EFO_1000403','EFO_1000416','EFO_1000419','EFO_1000448','EFO_1000453','EFO_1000454','EFO_1000464','EFO_1000465','EFO_1000467','EFO_1000470','EFO_1000478','EFO_1000485','EFO_1000489','EFO_1000516','EFO_1000520','EFO_1000521','EFO_1000531','EFO_1000532','EFO_1000541','EFO_1000546','EFO_1000553','EFO_1000556','EFO_1000566','EFO_1000570','EFO_1000576','EFO_1000581','EFO_1000585','EFO_1000599','EFO_1000601','EFO_1000606','EFO_1000609','EFO_1000613','EFO_1000616','EFO_1000624','EFO_1000627','EFO_1000630','EFO_1000634','EFO_1000635','EFO_1000639','EFO_1000646','EFO_1000650','EFO_1000654','EFO_1000657','EFO_1000717','EFO_1000720','EFO_1000728','EFO_1000763','EFO_1000772','EFO_1000779','EFO_1000796','EFO_1000797','EFO_1000876','EFO_1000886','EFO_1000889','EFO_1000896','EFO_1000920','EFO_1000934','EFO_1000973','EFO_1000979','EFO_1000981','EFO_1000999','EFO_1001035','EFO_1001044','EFO_1001047','EFO_1001049','EFO_1001052','EFO_1001073','EFO_1001087','EFO_1001094','EFO_1001100','EFO_1001171','EFO_1001172','EFO_1001183','EFO_1001184','EFO_1001185','EFO_1001204','EFO_1001214','EFO_1001216','EFO_1001230','EFO_1001339','EFO_1001357','EFO_1001434','EFO_1001437','EFO_1001443','EFO_1001447','EFO_1001455','EFO_1001463','EFO_1001469','EFO_1001502','EFO_1001512','EFO_1001513','EFO_1001779','EFO_1001853','EFO_1001875','EFO_1001901','EFO_1001902','EFO_1001928','EFO_1001931','EFO_1001937','EFO_1001938','EFO_1001940','EFO_1001946','EFO_1001949','EFO_1001950','EFO_1001951','EFO_1001956','EFO_1001961','EFO_1001962','EFO_1001965','EFO_1001967','EFO_1001968','EFO_1001972','EFO_1001986','EFO_1002017','EFO_1002018','EFO_1002023','EFO_1002046','EFO_1002049','HP_0000118','HP_0000707','HP_0000951','HP_0001574','MONDO_0000147','MONDO_0000148','MONDO_0000275','MONDO_0000314','MONDO_0000376','MONDO_0000380','MONDO_0000382','MONDO_0000383','MONDO_0000385','MONDO_0000386','MONDO_0000425','MONDO_0000426','MONDO_0000429','MONDO_0000430','MONDO_0000473','MONDO_0000474','MONDO_0000490','MONDO_0000502','MONDO_0000508','MONDO_0000514','MONDO_0000515','MONDO_0000521','MONDO_0000524','MONDO_0000527','MONDO_0000530','MONDO_0000535','MONDO_0000540','MONDO_0000544','MONDO_0000548','MONDO_0000551','MONDO_0000577','MONDO_0000591','MONDO_0000594','MONDO_0000611','MONDO_0000612','MONDO_0000618','MONDO_0000620','MONDO_0000621','MONDO_0000624','MONDO_0000625','MONDO_0000627','MONDO_0000628','MONDO_0000629','MONDO_0000631','MONDO_0000632','MONDO_0000633','MONDO_0000634','MONDO_0000636','MONDO_0000637','MONDO_0000638','MONDO_0000640','MONDO_0000646','MONDO_0000648','MONDO_0000649','MONDO_0000650','MONDO_0000651','MONDO_0000652','MONDO_0000653','MONDO_0000654','MONDO_0000709','MONDO_0000761','MONDO_0000831','MONDO_0000833','MONDO_0000839','MONDO_0000870','MONDO_0000873','MONDO_0000881','MONDO_0000919','MONDO_0000920','MONDO_0000921','MONDO_0000931','MONDO_0000952','MONDO_0000956','MONDO_0001014','MONDO_0001018','MONDO_0001023','MONDO_0001056','MONDO_0001059','MONDO_0001063','MONDO_0001082','MONDO_0001128','MONDO_0001174','MONDO_0001187','MONDO_0001235','MONDO_0001256','MONDO_0001322','MONDO_0001325','MONDO_0001340','MONDO_0001378','MONDO_0001402','MONDO_0001406','MONDO_0001407','MONDO_0001416','MONDO_0001422','MONDO_0001433','MONDO_0001475','MONDO_0001487','MONDO_0001502','MONDO_0001528','MONDO_0001572','MONDO_0001597','MONDO_0001657','MONDO_0001672','MONDO_0001704','MONDO_0001713','MONDO_0001724','MONDO_0001748','MONDO_0001763','MONDO_0001764','MONDO_0001770','MONDO_0001806','MONDO_0001834','MONDO_0001852','MONDO_0001879','MONDO_0001898','MONDO_0001926','MONDO_0001933','MONDO_0002013','MONDO_0002025','MONDO_0002031','MONDO_0002033','MONDO_0002035','MONDO_0002036','MONDO_0002037','MONDO_0002038','MONDO_0002058','MONDO_0002060','MONDO_0002071','MONDO_0002087','MONDO_0002090','MONDO_0002093','MONDO_0002095','MONDO_0002100','MONDO_0002108','MONDO_0002116','MONDO_0002120','MONDO_0002123','MONDO_0002129','MONDO_0002132','MONDO_0002135','MONDO_0002146','MONDO_0002149','MONDO_0002150','MONDO_0002158','MONDO_0002165','MONDO_0002167','MONDO_0002171','MONDO_0002176','MONDO_0002178','MONDO_0002181','MONDO_0002185','MONDO_0002187','MONDO_0002191','MONDO_0002195','MONDO_0002206','MONDO_0002217','MONDO_0002225','MONDO_0002229','MONDO_0002232','MONDO_0002236','MONDO_0002243','MONDO_0002256','MONDO_0002259','MONDO_0002273','MONDO_0002280','MONDO_0002289','MONDO_0002297','MONDO_0002300','MONDO_0002320','MONDO_0002328','MONDO_0002334','MONDO_0002351','MONDO_0002352','MONDO_0002353','MONDO_0002358','MONDO_0002360','MONDO_0002363','MONDO_0002366','MONDO_0002367','MONDO_0002369','MONDO_0002372','MONDO_0002373','MONDO_0002380','MONDO_0002395','MONDO_0002397','MONDO_0002402','MONDO_0002407','MONDO_0002415','MONDO_0002427','MONDO_0002429','MONDO_0002433','MONDO_0002454','MONDO_0002460','MONDO_0002463','MONDO_0002464','MONDO_0002466','MONDO_0002472','MONDO_0002475','MONDO_0002477','MONDO_0002480','MONDO_0002481','MONDO_0002482','MONDO_0002486','MONDO_0002488','MONDO_0002490','MONDO_0002493','MONDO_0002494','MONDO_0002510','MONDO_0002512','MONDO_0002513','MONDO_0002516','MONDO_0002527','MONDO_0002528','MONDO_0002529','MONDO_0002532','MONDO_0002533','MONDO_0002537','MONDO_0002542','MONDO_0002547','MONDO_0002564','MONDO_0002567','MONDO_0002580','MONDO_0002586','MONDO_0002597','MONDO_0002601','MONDO_0002603','MONDO_0002604','MONDO_0002616','MONDO_0002628','MONDO_0002629','MONDO_0002631','MONDO_0002633','MONDO_0002635','MONDO_0002654','MONDO_0002661','MONDO_0002664','MONDO_0002665','MONDO_0002676','MONDO_0002677','MONDO_0002678','MONDO_0002691','MONDO_0002715','MONDO_0002718','MONDO_0002720','MONDO_0002722','MONDO_0002727','MONDO_0002730','MONDO_0002732','MONDO_0002742','MONDO_0002746','MONDO_0002749','MONDO_0002785','MONDO_0002786','MONDO_0002798','MONDO_0002805','MONDO_0002807','MONDO_0002813','MONDO_0002814','MONDO_0002816','MONDO_0002817','MONDO_0002837','MONDO_0002847','MONDO_0002848','MONDO_0002852','MONDO_0002854','MONDO_0002866','MONDO_0002871','MONDO_0002872','MONDO_0002874','MONDO_0002879','MONDO_0002882','MONDO_0002883','MONDO_0002886','MONDO_0002887','MONDO_0002898','MONDO_0002912','MONDO_0002913','MONDO_0002914','MONDO_0002915','MONDO_0002917','MONDO_0002924','MONDO_0002927','MONDO_0002928','MONDO_0002930','MONDO_0002970','MONDO_0002973','MONDO_0002974','MONDO_0002979','MONDO_0002989','MONDO_0002995','MONDO_0002998','MONDO_0003000','MONDO_0003001','MONDO_0003002','MONDO_0003008','MONDO_0003036','MONDO_0003044','MONDO_0003059','MONDO_0003060','MONDO_0003061','MONDO_0003062','MONDO_0003064','MONDO_0003079','MONDO_0003081','MONDO_0003090','MONDO_0003098','MONDO_0003107','MONDO_0003110','MONDO_0003111','MONDO_0003112','MONDO_0003113','MONDO_0003119','MONDO_0003142','MONDO_0003159','MONDO_0003165','MONDO_0003169','MONDO_0003175','MONDO_0003190','MONDO_0003193','MONDO_0003194','MONDO_0003196','MONDO_0003197','MONDO_0003199','MONDO_0003204','MONDO_0003212','MONDO_0003215','MONDO_0003218','MONDO_0003222','MONDO_0003225','MONDO_0003236','MONDO_0003237','MONDO_0003241','MONDO_0003249','MONDO_0003252','MONDO_0003257','MONDO_0003268','MONDO_0003274','MONDO_0003275','MONDO_0003276','MONDO_0003277','MONDO_0003295','MONDO_0003321','MONDO_0003331','MONDO_0003342','MONDO_0003354','MONDO_0003363','MONDO_0003393','MONDO_0003403','MONDO_0003408','MONDO_0003409','MONDO_0003413','MONDO_0003424','MONDO_0003429','MONDO_0003430','MONDO_0003438','MONDO_0003443','MONDO_0003454','MONDO_0003478','MONDO_0003495','MONDO_0003500','MONDO_0003510','MONDO_0003512','MONDO_0003537','MONDO_0003541','MONDO_0003544','MONDO_0003549','MONDO_0003569','MONDO_0003578','MONDO_0003603','MONDO_0003604','MONDO_0003606','MONDO_0003641','MONDO_0003646','MONDO_0003649','MONDO_0003659','MONDO_0003660','MONDO_0003661','MONDO_0003685','MONDO_0003686','MONDO_0003688','MONDO_0003715','MONDO_0003719','MONDO_0003724','MONDO_0003750','MONDO_0003751','MONDO_0003755','MONDO_0003756','MONDO_0003762','MONDO_0003766','MONDO_0003778','MONDO_0003812','MONDO_0003837','MONDO_0003869','MONDO_0003890','MONDO_0003916','MONDO_0003939','MONDO_0003987','MONDO_0004007','MONDO_0004021','MONDO_0004033','MONDO_0004041','MONDO_0004095','MONDO_0004111','MONDO_0004180','MONDO_0004192','MONDO_0004202','MONDO_0004245','MONDO_0004251','MONDO_0004338','MONDO_0004355','MONDO_0004380','MONDO_0004403','MONDO_0004427','MONDO_0004479','MONDO_0004526','MONDO_0004532','MONDO_0004580','MONDO_0004600','MONDO_0004634','MONDO_0004641','MONDO_0004643','MONDO_0004647','MONDO_0004658','MONDO_0004663','MONDO_0004669','MONDO_0004685','MONDO_0004693','MONDO_0004695','MONDO_0004698','MONDO_0004699','MONDO_0004700','MONDO_0004724','MONDO_0004727','MONDO_0004748','MONDO_0004756','MONDO_0004805','MONDO_0004821','MONDO_0004830','MONDO_0004867','MONDO_0004928','MONDO_0004943','MONDO_0005094','MONDO_0005232','MONDO_0005374','MONDO_0005411','MONDO_0005499','MONDO_0007052','MONDO_0007254','MONDO_0007573','MONDO_0007576','MONDO_0007763','MONDO_0008093','MONDO_0008170','MONDO_0008277','MONDO_0008315','MONDO_0008627','MONDO_0008903','MONDO_0009229','MONDO_0009332','MONDO_0009348','MONDO_0009453','MONDO_0010584','MONDO_0010795','MONDO_0010837','MONDO_0011118','MONDO_0011962','MONDO_0012825','MONDO_0015062','MONDO_0015064','MONDO_0015065','MONDO_0015066','MONDO_0015067','MONDO_0015070','MONDO_0015077','MONDO_0015078','MONDO_0015081','MONDO_0015111','MONDO_0015118','MONDO_0015119','MONDO_0015126','MONDO_0015157','MONDO_0015161','MONDO_0015185','MONDO_0015194','MONDO_0015277','MONDO_0015319','MONDO_0015405','MONDO_0015475','MONDO_0015476','MONDO_0015514','MONDO_0015531','MONDO_0015667','MONDO_0015682','MONDO_0015683','MONDO_0015686','MONDO_0015748','MONDO_0015756','MONDO_0015757','MONDO_0015758','MONDO_0015760','MONDO_0015798','MONDO_0015816','MONDO_0015817','MONDO_0015818','MONDO_0015819','MONDO_0015820','MONDO_0015821','MONDO_0015822','MONDO_0015851','MONDO_0015853','MONDO_0015860','MONDO_0015861','MONDO_0015864','MONDO_0015867','MONDO_0015876','MONDO_0015881','MONDO_0015889','MONDO_0015913','MONDO_0015917','MONDO_0016093','MONDO_0016096','MONDO_0016123','MONDO_0016167','MONDO_0016180','MONDO_0016223','MONDO_0016230','MONDO_0016231','MONDO_0016233','MONDO_0016235','MONDO_0016238','MONDO_0016255','MONDO_0016275','MONDO_0016277','MONDO_0016286','MONDO_0016507','MONDO_0016508','MONDO_0016586','MONDO_0016635','MONDO_0016637','MONDO_0016680','MONDO_0016685','MONDO_0016691','MONDO_0016697','MONDO_0016701','MONDO_0016704','MONDO_0016708','MONDO_0016713','MONDO_0016715','MONDO_0016717','MONDO_0016721','MONDO_0016726','MONDO_0016729','MONDO_0016738','MONDO_0016744','MONDO_0016747','MONDO_0016748','MONDO_0016749','MONDO_0016752','MONDO_0016756','MONDO_0016784','MONDO_0016974','MONDO_0017026','MONDO_0017027','MONDO_0017120','MONDO_0017166','MONDO_0017167','MONDO_0017207','MONDO_0017341','MONDO_0017342','MONDO_0017343','MONDO_0017345','MONDO_0017582','MONDO_0017594','MONDO_0017595','MONDO_0017611','MONDO_0017631','MONDO_0017769','MONDO_0017795','MONDO_0017814','MONDO_0017820','MONDO_0017824','MONDO_0017955','MONDO_0018035','MONDO_0018079','MONDO_0018171','MONDO_0018191','MONDO_0018201','MONDO_0018202','MONDO_0018234','MONDO_0018235','MONDO_0018271','MONDO_0018352','MONDO_0018365','MONDO_0018386','MONDO_0018397','MONDO_0018502','MONDO_0018506','MONDO_0018511','MONDO_0018515','MONDO_0018520','MONDO_0018521','MONDO_0018530','MONDO_0018531','MONDO_0018532','MONDO_0018534','MONDO_0018536','MONDO_0018538','MONDO_0018539','MONDO_0018555','MONDO_0018630','MONDO_0018718','MONDO_0018719','MONDO_0018722','MONDO_0018729','MONDO_0018744','MONDO_0018751','MONDO_0018791','MONDO_0018792','MONDO_0018798','MONDO_0018898','MONDO_0018918','MONDO_0018972','MONDO_0019004','MONDO_0019042','MONDO_0019054','MONDO_0019061','MONDO_0019063','MONDO_0019086','MONDO_0019116','MONDO_0019268','MONDO_0019269','MONDO_0019277','MONDO_0019278','MONDO_0019293','MONDO_0019296','MONDO_0019304','MONDO_0019404','MONDO_0019453','MONDO_0019460','MONDO_0019473','MONDO_0019500','MONDO_0019590','MONDO_0019717','MONDO_0019748','MONDO_0019751','MONDO_0019755','MONDO_0019781','MONDO_0019832','MONDO_0019833','MONDO_0019859','MONDO_0019927','MONDO_0019937','MONDO_0019962','MONDO_0019964','MONDO_0020006','MONDO_0020013','MONDO_0020014','MONDO_0020022','MONDO_0020035','MONDO_0020077','MONDO_0020078','MONDO_0020082','MONDO_0020083','MONDO_0020172','MONDO_0020173','MONDO_0020511','MONDO_0020516','MONDO_0020539','MONDO_0020550','MONDO_0020561','MONDO_0020574','MONDO_0020580','MONDO_0020588','MONDO_0020590','MONDO_0020592','MONDO_0020596','MONDO_0020606','MONDO_0020629','MONDO_0020633','MONDO_0020639','MONDO_0020641','MONDO_0020644','MONDO_0020653','MONDO_0020654','MONDO_0020663','MONDO_0020665','MONDO_0020669','MONDO_0020676','MONDO_0020687','MONDO_0020703','MONDO_0020760','MONDO_0021009','MONDO_0021038','MONDO_0021043','MONDO_0021046','MONDO_0021047','MONDO_0021049','MONDO_0021052','MONDO_0021053','MONDO_0021054','MONDO_0021057','MONDO_0021058','MONDO_0021063','MONDO_0021064','MONDO_0021065','MONDO_0021066','MONDO_0021067','MONDO_0021069','MONDO_0021072','MONDO_0021074','MONDO_0021075','MONDO_0021076','MONDO_0021077','MONDO_0021078','MONDO_0021079','MONDO_0021080','MONDO_0021084','MONDO_0021086','MONDO_0021089','MONDO_0021091','MONDO_0021092','MONDO_0021094','MONDO_0021096','MONDO_0021110','MONDO_0021114','MONDO_0021117','MONDO_0021118','MONDO_0021119','MONDO_0021120','MONDO_0021121','MONDO_0021138','MONDO_0021143','MONDO_0021144','MONDO_0021148','MONDO_0021154','MONDO_0021165','MONDO_0021169','MONDO_0021179','MONDO_0021191','MONDO_0021192','MONDO_0021193','MONDO_0021205','MONDO_0021218','MONDO_0021222','MONDO_0021224','MONDO_0021225','MONDO_0021228','MONDO_0021230','MONDO_0021231','MONDO_0021232','MONDO_0021233','MONDO_0021237','MONDO_0021238','MONDO_0021239','MONDO_0021243','MONDO_0021246','MONDO_0021248','MONDO_0021249','MONDO_0021250','MONDO_0021251','MONDO_0021254','MONDO_0021257','MONDO_0021258','MONDO_0021259','MONDO_0021271','MONDO_0021303','MONDO_0021310','MONDO_0021311','MONDO_0021312','MONDO_0021313','MONDO_0021316','MONDO_0021320','MONDO_0021321','MONDO_0021322','MONDO_0021327','MONDO_0021331','MONDO_0021335','MONDO_0021337','MONDO_0021343','MONDO_0021345','MONDO_0021348','MONDO_0021350','MONDO_0021351','MONDO_0021353','MONDO_0021354','MONDO_0021355','MONDO_0021358','MONDO_0021360','MONDO_0021364','MONDO_0021366','MONDO_0021368','MONDO_0021370','MONDO_0021374','MONDO_0021375','MONDO_0021381','MONDO_0021383','MONDO_0021385','MONDO_0021386','MONDO_0021392','MONDO_0021400','MONDO_0021416','MONDO_0021440','MONDO_0021450','MONDO_0021451','MONDO_0021452','MONDO_0021454','MONDO_0021460','MONDO_0021463','MONDO_0021468','MONDO_0021489','MONDO_0021490','MONDO_0021499','MONDO_0021501','MONDO_0021510','MONDO_0021511','MONDO_0021525','MONDO_0021533','MONDO_0021539','MONDO_0021545','MONDO_0021546','MONDO_0021580','MONDO_0021581','MONDO_0021582','MONDO_0021583','MONDO_0021605','MONDO_0021629','MONDO_0021631','MONDO_0021632','MONDO_0021634','MONDO_0021637','MONDO_0021638','MONDO_0021639','MONDO_0021640','MONDO_0021652','MONDO_0021656','MONDO_0021657','MONDO_0021659','MONDO_0021662','MONDO_0021681','MONDO_0021682','MONDO_0021698','MONDO_0021699','MONDO_0021945','MONDO_0023113','MONDO_0023206','MONDO_0023369','MONDO_0023370','MONDO_0023603','MONDO_0023644','MONDO_0024240','MONDO_0024247','MONDO_0024255','MONDO_0024276','MONDO_0024282','MONDO_0024286','MONDO_0024292','MONDO_0024294','MONDO_0024296','MONDO_0024297','MONDO_0024311','MONDO_0024320','MONDO_0024322','MONDO_0024337','MONDO_0024338','MONDO_0024339','MONDO_0024341','MONDO_0024355','MONDO_0024387','MONDO_0024417','MONDO_0024458','MONDO_0024462','MONDO_0024468','MONDO_0024469','MONDO_0024470','MONDO_0024474','MONDO_0024475','MONDO_0024476','MONDO_0024477','MONDO_0024478','MONDO_0024479','MONDO_0024481','MONDO_0024482','MONDO_0024499','MONDO_0024500','MONDO_0024501','MONDO_0024502','MONDO_0024503','MONDO_0024572','MONDO_0024573','MONDO_0024582','MONDO_0024611','MONDO_0024615','MONDO_0024621','MONDO_0024622','MONDO_0024625','MONDO_0024634','MONDO_0024635','MONDO_0024637','MONDO_0024645','MONDO_0024653','MONDO_0024654','MONDO_0024656','MONDO_0024660','MONDO_0024661','MONDO_0024662','MONDO_0024666','MONDO_0024757','MONDO_0024813','MONDO_0024876','MONDO_0024878','MONDO_0024879','MONDO_0024880','MONDO_0024882','MONDO_0024885','MONDO_0024886','MONDO_0024890','MONDO_0025511','MONDO_0027772','MONDO_0036511','MONDO_0036591','MONDO_0036688','MONDO_0036696','MONDO_0036870','MONDO_0036976','MONDO_0037003','MONDO_0037254','MONDO_0037255','MONDO_0037256','MONDO_0037735','MONDO_0037736','MONDO_0037743','MONDO_0037745','MONDO_0037746','MONDO_0037792','MONDO_0037940','MONDO_0040675','MONDO_0040677','MONDO_0040678','MONDO_0042493','MONDO_0042982','MONDO_0042983','MONDO_0043007','MONDO_0043218','MONDO_0043579','MONDO_0043707','MONDO_0044334','MONDO_0044335','MONDO_0044704','MONDO_0044705','MONDO_0044710','MONDO_0044743','MONDO_0044794','MONDO_0044881','MONDO_0044887','MONDO_0044919','MONDO_0044925','MONDO_0044926','MONDO_0044937','MONDO_0044964','MONDO_0044983','MONDO_0044986','MONDO_0044991','MONDO_0044992','MONDO_0044993','MONDO_0044995','MONDO_0045010','MONDO_0045011','MONDO_0045012','MONDO_0045024','MONDO_0045044','MONDO_0045058','MONDO_0045063','MONDO_0045069','MONDO_0045070','MONDO_0056799','MONDO_0056804','MONDO_0056806','MONDO_0056815','MONDO_0056819','MONDO_0056820','MONDO_0100038','MONDO_0100070','Orphanet_100094','Orphanet_101435','Orphanet_101953','Orphanet_101957','Orphanet_101988','Orphanet_101997','Orphanet_102020','Orphanet_108987','Orphanet_113','Orphanet_116','Orphanet_117573','Orphanet_1359','Orphanet_137','Orphanet_138050','Orphanet_139009','Orphanet_139027','Orphanet_140162','Orphanet_141132','Orphanet_144','Orphanet_155896','Orphanet_156207','Orphanet_156237','Orphanet_156532','Orphanet_156610','Orphanet_156619','Orphanet_156629','Orphanet_156638','Orphanet_158300','Orphanet_165652','Orphanet_165655','Orphanet_165658','Orphanet_165661','Orphanet_166466','Orphanet_166472','Orphanet_166487','Orphanet_168778','Orphanet_169346','Orphanet_169361','Orphanet_174590','Orphanet_176','Orphanet_1775','Orphanet_178','Orphanet_178025','Orphanet_179006','Orphanet_180772','Orphanet_181390','Orphanet_181437','Orphanet_182040','Orphanet_183422','Orphanet_183426','Orphanet_183435','Orphanet_183447','Orphanet_183450','Orphanet_183463','Orphanet_183466','Orphanet_183472','Orphanet_183478','Orphanet_183481','Orphanet_183484','Orphanet_183487','Orphanet_183490','Orphanet_183497','Orphanet_183500','Orphanet_183503','Orphanet_183506','Orphanet_183512','Orphanet_183521','Orphanet_183524','Orphanet_183527','Orphanet_183530','Orphanet_183533','Orphanet_183536','Orphanet_183539','Orphanet_183545','Orphanet_183557','Orphanet_183570','Orphanet_183573','Orphanet_183576','Orphanet_183580','Orphanet_183583','Orphanet_183595','Orphanet_183598','Orphanet_183607','Orphanet_183619','Orphanet_183628','Orphanet_183634','Orphanet_183637','Orphanet_183643','Orphanet_183651','Orphanet_183654','Orphanet_183731','Orphanet_183734','Orphanet_183757','Orphanet_183763','Orphanet_183770','Orphanet_202940','Orphanet_206634','Orphanet_208596','Orphanet_211240','Orphanet_213517','Orphanet_217595','Orphanet_220460','Orphanet_2207','Orphanet_222628','Orphanet_227535','Orphanet_228184','Orphanet_235832','Orphanet_235936','Orphanet_238468','Orphanet_238510','Orphanet_240371','Orphanet_247709','Orphanet_2495','Orphanet_250165','Orphanet_2549','Orphanet_261786','Orphanet_261816','Orphanet_261947','Orphanet_262038','Orphanet_262653','Orphanet_262785','Orphanet_263708','Orphanet_269564','Orphanet_269567','Orphanet_271832','Orphanet_271835','Orphanet_271841','Orphanet_271844','Orphanet_271847','Orphanet_271870','Orphanet_275742','Orphanet_276161','Orphanet_281085','Orphanet_281210','Orphanet_2869','Orphanet_306498','Orphanet_306661','Orphanet_306765','Orphanet_307061','Orphanet_309005','Orphanet_309447','Orphanet_309450','Orphanet_309458','Orphanet_319328','Orphanet_319465','Orphanet_319494','Orphanet_322126','Orphanet_325638','Orphanet_325665','Orphanet_325690','Orphanet_325706','Orphanet_330197','Orphanet_330206','Orphanet_331193','Orphanet_331217','Orphanet_3389','Orphanet_34533','Orphanet_35173','Orphanet_359','Orphanet_363294','Orphanet_363314','Orphanet_364526','Orphanet_364803','Orphanet_371195','Orphanet_371200','Orphanet_371235','Orphanet_371436','Orphanet_371861','Orphanet_398934','Orphanet_399839','Orphanet_399980','Orphanet_399983','Orphanet_400008','Orphanet_400011','Orphanet_404568','Orphanet_404571','Orphanet_404577','Orphanet_404584','Orphanet_467','Orphanet_53715','Orphanet_538','Orphanet_55881','Orphanet_59305','Orphanet_618','Orphanet_64755','Orphanet_653','Orphanet_654','Orphanet_68335','Orphanet_68336','Orphanet_68346','Orphanet_68367','Orphanet_68383','Orphanet_71859','Orphanet_71862','Orphanet_733','Orphanet_77','Orphanet_77240','Orphanet_77828','Orphanet_790','Orphanet_79161','Orphanet_79195','Orphanet_79226','Orphanet_79360','Orphanet_79366','Orphanet_79373','Orphanet_79383','Orphanet_79385','Orphanet_79387','Orphanet_79452','Orphanet_79493','Orphanet_89832','Orphanet_90642','Orphanet_93442','Orphanet_93444','Orphanet_93447','Orphanet_93449','Orphanet_93450','Orphanet_93453','Orphanet_93459','Orphanet_93460','Orphanet_93547','Orphanet_93587','Orphanet_95488','Orphanet_95494','Orphanet_96210','Orphanet_96333','Orphanet_96346','Orphanet_98054','Orphanet_98056','Orphanet_98087','Orphanet_98127','Orphanet_98130','Orphanet_98132','Orphanet_98142','Orphanet_98152','Orphanet_98154','Orphanet_98196','Orphanet_98464','Orphanet_98497','Orphanet_98554','Orphanet_98557','Orphanet_98560','Orphanet_98561','Orphanet_98564','Orphanet_98566','Orphanet_98580','Orphanet_98583','Orphanet_98584','Orphanet_98585','Orphanet_98586','Orphanet_98587','Orphanet_98588','Orphanet_98589','Orphanet_98590','Orphanet_98591','Orphanet_98592','Orphanet_98594','Orphanet_98598','Orphanet_98601','Orphanet_98602','Orphanet_98603','Orphanet_98604','Orphanet_98605','Orphanet_98610','Orphanet_98611','Orphanet_98612','Orphanet_98614','Orphanet_98615','Orphanet_98616','Orphanet_98617','Orphanet_98628','Orphanet_98631','Orphanet_98632','Orphanet_98634','Orphanet_98638','Orphanet_98640','Orphanet_98641','Orphanet_98643','Orphanet_98648','Orphanet_98649','Orphanet_98655','Orphanet_98657','Orphanet_98668','Orphanet_98669','Orphanet_98696','Orphanet_98697','Orphanet_98698','Orphanet_98699','Orphanet_98700','Orphanet_98701','Orphanet_98703','Orphanet_98708','Orphanet_98709','Orphanet_98733','Orphanet_99739','OTAR_0000006','OTAR_0000009','OTAR_0000010','OTAR_0000014','OTAR_0000017','OTAR_0000018']) as As
            SELECT As
            )) AND datasource_id not in ('expression_atlas'))
         OR (A = 'EFO_0000616' and datasource_id in ('expression_atlas'))
    GROUP BY
             B,
             datasource_id
--    HAVING (B_data LIKE '%BRCA%')
    )
GROUP BY B
HAVING score_overall > 0
ORDER BY score_overall DESC
LIMIT 100;
