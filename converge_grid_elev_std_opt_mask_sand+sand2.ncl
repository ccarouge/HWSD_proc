;*******************************************
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/shea_util.ncl"
;*******************************************

; changes from converge_grid_elev_std_LAI.ncl
; 1. fix the type conflict of lat&lon between gridfile (double) and elevfile (float), finally use float
; 2. add Mark's elevfile equations as an option to calculate soil parameters
; 3. clarify script's structure
; 4. change hyds unit from mm to m, while hyds_vec still uses mm
; 5. add standard LAI (comment out SnowDepth and SoilTemp, because the standard one might make things worse)
;    to replace Mark's gridinfo one
; 6. add another interpolation to avoid land area losing due to missing_value expantion
; 7. fout->landsea@units = "0:land, 1:sea"
; 8. add using std isoil(/LAI/SnowDepth/SoilTemp, added before) as options
; 9. add _FillValue for the variables who don't have one
; 10. mask out no-land area of all 2D 3D 4D variable
; 11. use clay/silt/sand/organic to hold the weighted mean of 0-1m soil layers.
;     clay2/silt2/sand2/organic2 hold the 0.3-1m soil layer,
;     and the pixels clay2+silt2+sand2 < 0.9 are replaced by clay/silt/sand.
;     clay_vec/silt_vec/sand_vec/organic_vec = clay/silt/sand/organic.
; 12. change the type of iveg and isoil into integer
; 13. change soil_depth from 0-2m to (/ .011, .051, .157, .4385, 1.1855, 3.164 /)
; 14. change longitude:units = "degrees_East" and latitude:units = "degrees_North"

;______ From Mark Decker's code ______
setvalues NhlGetWorkspaceObjectId()
  "wsMaximumSize" : 5000000000 ;
end setvalues
;_____________________________________

begin


;________________________________ Options ________________________________
ip_opt = 1
;  For LAI and isoil
;  0 : using ncl bilinear interpolation function - linint2
;  1 : put the same standard value to every gridinfo cell in the standard grid,
;     and use landsea masks out spilting cells

std_opt = 1
;  0 : using isoil/LAI (/SnowDepth/SoilTemp,commented out) of Mark's gridinfo file
;  1 : using isoil/LAI (/SnowDepth/SoilTemp,commented out) of standard gridinfo file


;_______________________________ Pre-define ________________________________

file_name = (/\
         "/g/data/w35/ccc561/LIS-WRF/CABLE_soilparams/Harmonized_Global_Soil_Data/data",\ ; lat from - to + lon from -180 to 180
         "/g/data/w35/ccc561/LIS-WRF/CABLE_soilparams/CABLE_AUX-dev/CABLE_UNSW_GSWP3_gridinfo_0.5x0.5_site.nc",\ ;lat from + to - lon from -180 to 180
         "/g/data/w35/ccc561/LIS-WRF/CABLE_soilparams/CABLE_AUX-dev/GSWP3_gwmodel_parameters.nc",\ ;lat from - to + lon from 0 to 360
         "/g/data/w35/ccc561/LIS-WRF/CABLE_soilparams/CABLE_AUX-dev/gridinfo_CSIRO_1x1.nc"\ ;lat from + to - lon from -180 to 180
           /)

var_name1 = (/\
              "T_SAND","S_SAND","T_CLAY","S_CLAY","T_SILT","S_SILT","T_OC","S_OC"\
            /)

var_name2 = (/\
              "sand","sand2","clay","clay2","silt","silt2","organic","organic2"\
             /)

gridinfo_nml = (/\
              "iveg","isoil","time",\ ;"longitude","latitude",
              "x","y","rad_dummy","patch","rad","soil",\
              "patchfrac","soil_color","landsea",\
              "sand","sand2","silt","silt2","clay","clay2","organic","organic2",\
              "hyds","bch","sfc","ssat","sucs","swilt","css","cnsd",\
              "rhosoil","LAI","Albedo","albedo2",\
              "SnowDepth","SoilMoist","SoilTemp"\
              /)

cp_gridinfo = (/\
                "iveg","time","x","y","rad_dummy","patch","rad","soil",\
                "patchfrac",\ ;"longitude","latitude",
                "soil_color","landsea","rhosoil",\ ;"isoil","cnsd",
                "Albedo","albedo2",\ ; "LAI",
                "SoilMoist","SnowDepth","SoilTemp"\;
                /)

elev_nml = (/\
              "soil_depth","longitude","latitude",\
              "drainage_density","drainage_dist",\
              "slope","slope_std",\
              "Sy","dtb","elevation","elevation_std",\
              "permeability","permeability_std",\
              "bch_vec","sucs_vec","ssat_vec","swilt_vec","sfc_vec","css_vec","hyds_vec",\
              "rhosoil_vec","cnsd_vec",\
              "sand_vec","clay_vec","silt_vec","org_vec",\ ; top 3 ->sand/clay/silt lower 3 ->sand2/clay2/silt2
              "watr"\
           /)

cp_elev = (/\
             "longitude","latitude",\ ; "soil_depth",
             "Sy","drainage_density","drainage_dist",\
             "slope","slope_std",\
             "dtb","elevation","elevation_std",\
             "permeability","permeability_std"\ ;"watr"\
            /)

nml_2D = (/\
            "iveg","isoil","soil_color","rhosoil","Albedo","albedo2",\ ;2D "landsea",
            "sand","sand2","silt","silt2","clay","clay2","organic","organic2",\
            "hyds","bch","sfc","ssat","sucs","swilt","css","cnsd",\
            "drainage_density","drainage_dist",\
            "slope","slope_std","Sy","dtb","elevation","elevation_std",\
            "permeability","permeability_std"\
          /)
nml_3D = (/\
            "patchfrac","LAI",\ ;3D
            "SnowDepth","bch_vec","sucs_vec","ssat_vec","swilt_vec","sfc_vec","css_vec","hyds_vec",\
            "rhosoil_vec","cnsd_vec","sand_vec","clay_vec","silt_vec","org_vec","watr"\
           /)
nml_4D = (/"SoilMoist","SoilTemp"/)         ; 4D


add_FillValue =  (/\
            "soil_color","rhosoil","Albedo","albedo2","cnsd","watr",\
            "landsea","patchfrac","SnowDepth","SoilMoist","SoilTemp"\
            /)

; ______________________________ Forming nc data ____________________________
fcp = addfile (file_name(1),"r")
fcp2 = addfile(file_name(2),"r")

if eq_opt .eq. 0 then
  flag1 = "GADY-CABLE"
else if eq_opt .eq. 1 then
  flag1 = "MD_elev"
end if
end if

if ip_opt .eq. 0 then
  flag2 = "bilinear"
else if ip_opt .eq. 1 then
  flag2 = "orig"
end if
end if

if std_opt .eq. 0 then
  flag3 = ""
else if std_opt .eq. 1 then
  flag3 = "_std"
end if
end if

filo = "gridinfo_mmy_"+flag1+"_"+flag2+flag3+"_avg-sand_mask.nc"


system("/bin/rm -f " + filo)    ; remove if exists
fout  = addfile (filo, "c")     ; open output file

; explicitly declare file definition mode. Improve efficiency.
setfileoption(fout,"DefineMode",True)

; create global attributes of the file
fAtt               = True            ; assign file attributes
fAtt@title         = "converge gridinfo and elev"
if std_opt .eq. 0 then
  fAtt@source_file   = "CABLE_UNSW_GSWP3_gridinfo_0.5x0.5_site.nc and GSWP3_gwmodel_parameters.nc"
else if std_opt .eq. 1 then
  fAtt@source_file   = "CABLE_UNSW_GSWP3_gridinfo_0.5x0.5_site.nc, GSWP3_gwmodel_parameters.nc, and gridinfo_CSIRO_1x1.nc (LAI,isoil)"
end if
end if
fAtt@Conventions   = "None"
fAtt@creation_date = systemfunc ("date")
fileattdef( fout, fAtt )            ; copy file attributes

; predefine the coordinate variables and their dimensionality
; Note: to get an UNLIMITED record dimension, we set the dimensionality
; to -1 (or the actual size) and set the dimension Unlim to True.

nlat   = 360 ;
nlon   = 720
ntime  = 12 ;
nsoil  = 6 ;
npatch = 1 ;
nrad   = 3 ;
nsdepth = 6

dimNames = (/"time", "latitude", "longitude", "soil", "patch", "rad", "soil_depth"/)
dimSizes = (/ 12   ,  nlat     ,  nlon      , nsoil , npatch , nrad , nsdepth     /)
dimUnlim = (/ False,  False    ,  False     , False , False  , False, False       /)
filedimdef(fout,dimNames,dimSizes,dimUnlim)

; predefine the the dimensionality of the variables to be written out
do var_num = 0,34
  print("Predefine " + gridinfo_nml(var_num))
  if var_num .le. 1 then
    filevardef(fout, gridinfo_nml(var_num), "integer", getvardims(fcp->$gridinfo_nml(var_num)$))
  else
    filevardef(fout, gridinfo_nml(var_num),typeof(fcp->$gridinfo_nml(var_num)$),getvardims(fcp->$gridinfo_nml(var_num)$))
  end if
end do
;print(fout->iveg)

do var_num = 0,26
  print("Predefine " + elev_nml(var_num))
  filevardef(fout, elev_nml(var_num),typeof(fcp2->$elev_nml(var_num)$), getvardims(fcp2->$elev_nml(var_num)$))
end do

; Copy attributes associated with each variable to the file
; All attributes associated with each variable will be copied.

fout->iveg@_FillValue = -1

do var_num = 2,34
  print("Copy attributes " + gridinfo_nml(var_num))
  filevarattdef(fout,gridinfo_nml(var_num),fcp->$gridinfo_nml(var_num)$)
end do

do var_num = 0,26
  print("Copy attributes " + elev_nml(var_num))
  filevarattdef(fout,elev_nml(var_num),fcp2->$elev_nml(var_num)$)
end do


; ____________ add FillValue _____________
do var_num = 0,10
  fout->$add_FillValue(var_num)$@_FillValue = -9999.
end do

; explicitly exit file definition mode. **NOT REQUIRED**
setfileoption(fout,"DefineMode",False)
print("Exit DefineMode")

; output only the data values since the dimensionality and such have
; been predefined. The "(/", "/)" syntax tells NCL to only output the
; data values to the predefined locations on the file.

do var_num = 0,16
   print("Copy gridinfo "+cp_gridinfo(var_num))
   if var_num .eq. 0 then
      fout->$cp_gridinfo(var_num)$ = tointeger(fcp->$cp_gridinfo(var_num)$)
   else
      fout->$cp_gridinfo(var_num)$ = fcp->$cp_gridinfo(var_num)$*1
   end if
end do

fout->landsea@units = "0:land, 1:sea"

do var_num = 0,11
   print("Copy elev "+cp_elev(var_num))
   if var_num .le. 1 then
      fout->$cp_elev(var_num)$ = fcp2->$cp_elev(var_num)$
   else
      fout->$cp_elev(var_num)$ = lonFlip(fcp2->$cp_elev(var_num)$(::-1,:))
   end if
end do
fout->soil_depth= (/0.011,0.051,0.157,0.4385,1.1855,3.164/)
fout->latitude  = dble2flt(fcp->latitude) ;dble2flt doubletofloat
fout->longitude = dble2flt(fcp->longitude)

;____ change lon and lat 's units ____
fout->longitude@units = "degrees_East"
fout->latitude@units  = "degrees_North"

print(fout->latitude)
print(fout->longitude)

if eq_opt .eq. 0 then

   print("Copy gridinfo cnsd")
   fout->cnsd = fcp->cnsd*1

   print("Copy elev watr")
   fout->watr = lonFlip(fcp2->watr(:,::-1,:))

end if

if std_opt .eq. 0 then

   print("Copy gridinfo LAI")
   fout->LAI   = fcp->LAI*1

   print("Copy gridinfo isoil")
   fout->isoil = tointeger(fcp->isoil)

end if


delete(fcp)
delete(fcp2)

; _________________________ Check Value ____________________________
pic = "check_value"
wks = gsn_open_wks("pdf",pic)
gsn_define_colormap(wks,"WhiteBlueGreenYellowRed")
res            = True
res@cnFillMode = "RasterFill"            ; Raster Mode
res@cnFillOn   = True
res@cnLinesOn  = False

; _______________________ Standard input ____________________________
f2 = addfile(file_name(1),"r")
Lat = f2->latitude(::-1)
Lon = f2->longitude

if std_opt .eq. 1 then

f4 = addfile(file_name(3),"r")
Lat_std = fspan(89.5,-89.5,180)
Lon_std = f4->longitude

LAI_std   = new((/12,180,360/),  float)
isoil_std = new((/180,360/),  float)

LAI_std@_FillValue   = f4->LAI@missing_value
isoil_std@_FillValue = f4->isoil@missing_value

LAI_std(:,:149,:) =  f4->LAI*1.0
LAI_std(:,150:,:) =  LAI_std@_FillValue
LAI_std!0 = "time"
LAI_std!1 = "lat"
LAI_std!2 = "lon"
LAI_std&lat = Lat_std
LAI_std&lon = Lon_std

isoil_std(:149,:) =  f4->isoil*1
isoil_std(150:,:) =  isoil_std@_FillValue
isoil_std!0 = "lat"
isoil_std!1 = "lon"
isoil_std&lat = Lat_std
isoil_std&lon = Lon_std

LAI       = new((/12,360,720/),  float)
isoil     = new((/360,720/),  integer)

if ip_opt .eq. 0 then

  LAI   = linint2(Lon_std,Lat_std(::-1),LAI_std(:,::-1,:),True,Lon,Lat,0);_Wrap
  isoil = tointeger(linint2(Lon_std,Lat_std(::-1),isoil_std(::-1,:),True,Lon,Lat,0))
  isoil@_FillValue = -1

  fout->LAI   = LAI(:,::-1,:)
  fout->isoil = isoil(::-1,:)

else if ip_opt .eq. 1 then
  LAI       = new((/12,360,720/),  float)
  isoil     = new((/360,720/),  integer)

  LAI@_FillValue       = f4->LAI@missing_value
  isoil@_FillValue     = -1 ;f4->isoil@missing_value

  do latt = 0,179
    do lonn = 0,359
        LAI(:,latt*2,lonn*2)     = LAI_std(:,latt,lonn)
        LAI(:,latt*2+1,lonn*2)   = LAI_std(:,latt,lonn)
        LAI(:,latt*2,lonn*2+1)   = LAI_std(:,latt,lonn)
        LAI(:,latt*2+1,lonn*2+1) = LAI_std(:,latt,lonn)

        isoil(latt*2,lonn*2)     = tointeger(isoil_std(latt,lonn))
        isoil(latt*2+1,lonn*2)   = tointeger(isoil_std(latt,lonn))
        isoil(latt*2,lonn*2+1)   = tointeger(isoil_std(latt,lonn))
        isoil(latt*2+1,lonn*2+1) = tointeger(isoil_std(latt,lonn))

    end do
  end do

  do nn = 0,11
    LAI(nn,:,:)       = where(fout->landsea .eq. 0, LAI(nn,:,:), LAI@_FillValue)
  end do

  isoil = where(fout->landsea .eq. 0, isoil, isoil@_FillValue)

  fout->LAI = LAI*1.0
  fout->isoil = isoil

end if ; ip_opt
end if


plot = gsn_csm_contour(wks,fout->LAI(0,:,:)*1.0,res)
plot = gsn_csm_contour(wks,fout->isoil*1,res)

delete(f4)
delete(LAI)
delete(isoil)
delete(LAI_std)
delete(isoil_std)
delete(Lat_std)
delete(Lon_std)

print("finishing standard input")
end if ; std_opt

; ____________________Soil texture data input ______________________
sand    = new((/360,720/),float)
clay    = new((/360,720/),float)
silt    = new((/360,720/),float)
organic = new((/360,720/),float)
sand2    = new((/360,720/),float)
clay2    = new((/360,720/),float)
silt2    = new((/360,720/),float)
organic2 = new((/360,720/),float)
sand    = 0.
clay    = 0.
silt    = 0.
organic = 0.
sand2    = 0.
clay2    = 0.
silt2    = 0.
organic2 = 0.

do var_num = 0,7

var1 = new((/3600,7200/),float)

f1 = addfile(file_name(0)+"/"+var_name1(var_num)+".nc4","r")
var1 = dble2flt(f1->$var_name1(var_num)$)*1.0/100.;
var1@_FillValue = dble2flt(f1->$var_name1(var_num)$@_FillValue) ;7

;plot = gsn_csm_contour(wks,var1,res)

var1!0 = "lat"
var1!1 = "lon"
lat = dble2flt(f1->lat);(::-1)
lon = dble2flt(f1->lon)
var1&lat = lat
var1&lon = lon

;printVarSummary(var1)
print("point 1 ")
var3 = linint2_Wrap(lon,lat,var1,True,Lon,Lat,0)
delete(var1)
var3 = var3(::-1,:)
var3!0 = "lat" ;It doesn't need here
var3!1 = "lon"
var3&lat = Lat
var3&lon = Lon
print("point 2 ")

sand@_FillValue    = var3@_FillValue
clay@_FillValue    = var3@_FillValue
silt@_FillValue    = var3@_FillValue
organic@_FillValue = var3@_FillValue
sand2@_FillValue    = var3@_FillValue
clay2@_FillValue    = var3@_FillValue
silt2@_FillValue    = var3@_FillValue
organic2@_FillValue = var3@_FillValue
print("point 3 ")

if var_num .eq. 0 then
   sand = var3
else if var_num .eq. 1 then
   sand2 = var3
else if var_num .eq. 2 then
   clay = var3
else if var_num .le. 3 then
   clay2 = var3
else if var_num .le. 4 then
   silt = var3
else if var_num .le. 5 then
   slit2 = var3
else if var_num .le. 6 then
   organic = var3
else if var_num .le. 7 then
   organic2 = var3
end if
end if
end if
end if
end if
end if
end if
end if
delete(var3)
print("point 4 ")
end do

; fill missing pixels
sand2    = where((sand2+clay2+silt2) .lt. 0.9, sand, sand2)
clay2    = where((sand2+clay2+silt2) .lt. 0.9, clay, clay2)
silt2    = where((sand2+clay2+silt2) .lt. 0.9, silt, silt2)

sand    = sand*0.3+sand2*0.7 ; 0-0.3m is top soil layer, 0.3-1.0m is lower soil layer
clay    = clay*0.3+clay2*0.7
silt    = silt*0.3+silt2*0.7
organic = organic*0.3+organic2*0.7

; sand/clay/silt/organic hold the weighted mean of 0-1m soil layers
fout->sand = sand*1.0
fout->clay = clay*1.0
fout->silt = silt*1.0
fout->organic = organic*1.0
; sand2/clay2/silt2/organic2 hold the 0.3-1m soil layer
fout->sand2 = sand2*1.0
fout->clay2 = clay2*1.0
fout->silt2 = silt*1.0
fout->organic2 = organic2*1.0

delete(sand2)
delete(clay2)
delete(silt2)
delete(organic2)

print("finishing soil texture input")

plot = gsn_csm_contour(wks,fout->sand,res)
plot = gsn_csm_contour(wks,fout->clay,res)
plot = gsn_csm_contour(wks,fout->silt,res)
plot = gsn_csm_contour(wks,fout->organic,res)

plot = gsn_csm_contour(wks,fout->sand2,res)
plot = gsn_csm_contour(wks,fout->clay2,res)
plot = gsn_csm_contour(wks,fout->silt2,res)
plot = gsn_csm_contour(wks,fout->organic2,res)

res@tiMainString = "sand+clay+silt"
plot = gsn_csm_contour(wks,fout->sand+fout->clay+fout->silt,res)
res@tiMainString = "sand+clay+silt+organic"
plot = gsn_csm_contour(wks,fout->sand+fout->clay+fout->silt+fout->organic,res)
delete(res@tiMainString)

do layer = 0,5;2
fout->sand_vec(layer,:,:)   = fout->sand*1.0
fout->clay_vec(layer,:,:)   = fout->clay*1.0
fout->silt_vec(layer,:,:)   = fout->silt*1.0
fout->org_vec(layer,:,:)    = fout->organic*1.0
end do

plot = gsn_csm_contour(wks, dim_avg_n_Wrap(fout->sand_vec,0),res)
plot = gsn_csm_contour(wks, dim_avg_n_Wrap(fout->clay_vec,0),res)
plot = gsn_csm_contour(wks, dim_avg_n_Wrap(fout->silt_vec,0),res)
plot = gsn_csm_contour(wks, dim_avg_n_Wrap(fout->org_vec,0),res)

print("finishing soil texture output")

; __________________________ CALCULATION ____________________________
hyds  = new((/360,720/),float)
bch   = new((/360,720/),float)
sucs  = new((/360,720/),float)
ssat  = new((/360,720/),float)
swilt = new((/360,720/),float)
sfc   = new((/360,720/),float)
css   = new((/360,720/),float)

print("finishing allocation")

hyds@_FillValue  = -9999.
bch@_FillValue   = -9999.
sucs@_FillValue  = -9999.
ssat@_FillValue  = -9999.
swilt@_FillValue = -9999.
sfc@_FillValue   = -9999.
css@_FillValue   = -9999.

print("finishing setting missing value")

hyds!0 = "lat"
hyds!1 = "lon"
hyds&lat = Lat({::-1})
hyds&lon = Lon

bch!0 = "lat"
bch!1 = "lon"
bch&lat = Lat({::-1})
bch&lon = Lon

sucs!0 = "lat"
sucs!1 = "lon"
sucs&lat = Lat({::-1})
sucs&lon = Lon

ssat!0 = "lat"
ssat!1 = "lon"
ssat&lat = Lat({::-1})
ssat&lon = Lon

swilt!0 = "lat"
swilt!1 = "lon"
swilt&lat = Lat({::-1})
swilt&lon = Lon

sfc!0 = "lat"
sfc!1 = "lon"
sfc&lat = Lat({::-1})
sfc&lon = Lon

css!0 = "lat"
css!1 = "lon"
css&lat = Lat({::-1})
css&lon = Lon

print("finishing adding coordinate")

if eq_opt .eq. 1 then
; ________________________________ Mark Decker's equations for elev file ___________________________________
; From /g/data1/w35/mrd561/CABLE2_runs/CMIP6-GM2-testing/ssgw_Feb23.2018/opt/surface_data/add_derived_SWC.py
  watr  = new((/360,720/),float)
  cnsd  = new((/360,720/),float)

  watr@_FillValue  = -9999.
  cnsd@_FillValue  = -9999.

  watr!0 = "lat"
  watr!1 = "lon"
  watr&lat = Lat({::-1})
  watr&lon = Lon

  cnsd!0 = "lat"
  cnsd!1 = "lon"
  cnsd&lat = Lat({::-1})
  cnsd&lon = Lon

  hyds = (1.0-organic) * 0.00706 * ( 10.0 ^ (-0.60 + 1.26*sand - 0.64*clay) ) + organic*10^(-4)
  printVarSummary(hyds)

  bch  = (1.0-organic) * ( 3.1 + 15.7*clay - 0.3*sand ) + organic*3.0
  printVarSummary(bch)

  sucs = (1.0-organic) * 10.0 * 10.0^( 1.54 - 0.95*sand + 0.63*silt ) + organic*10.3
  printVarSummary(sucs)

  ssat = (1.0-organic) * ( 0.505 - 0.142*sand - 0.037*clay ) + organic*0.6
  printVarSummary(ssat)

  watr = (1.0-organic) * ( 0.02 + 0.018*clay ) + organic*0.15
  printVarSummary(watr)

  sst_tmp = ssat
  sst_tmp = where(sst_tmp .gt. 0.85, 0.85, sst_tmp)
  sst_tmp = where(sst_tmp .lt. 0.15, 0.15, sst_tmp)
  sst_tmp = 1.0 - sst_tmp
  printVarSummary(sst_tmp)

  iveg    = fout->iveg

  psi_tmp = ssat
  psi_tmp = 2750000.
  psi_tmp = where( iveg .le. 3, 2550000.0, psi_tmp)
  psi_tmp = where( iveg .eq. 4, 2240000.0, psi_tmp)
  psi_tmp = where( iveg .eq. 5, 4280000.0, psi_tmp)
  printVarSummary(psi_tmp)

  swilt = (ssat - watr) * ( (psi_tmp/sucs) ^ (-1.0/bch) ) + watr
  printVarSummary(swilt)

  sfc  = (ssat - watr) * ( 1.157407 * 10^(-06) / hyds) ^ (1.0 / (2.0*bch + 3.0) ) + watr
  printVarSummary(sfc)

  css  = (1.0-organic) * ( 850*(1.0 - sand - clay) + 865.0*clay + 750.0*sand ) + organic*950.0
  printVarSummary(css)

  cnsd = (1.0-organic) * ( 0.135*sst_tmp + 0.0239/sst_tmp )  / (1.0 - 0.947*sst_tmp) + organic*0.05
  printVarSummary(cnsd)

  delete(sand)
  delete(clay)
  delete(silt)
  delete(organic)
  print("finishing calculation")

; __________________ Note that ! ___________________
; Below are the soil properties for fully organic soil in
; ./Marks_latest_branch_with_fixes/core/biogeophys/cable_common.F90
; They are different from these parameters used in this script!
;        hyds_vec = 1.0e-4,&
;        sucs_vec = 10.3,   &
;        bch_vec  = 2.91,     &
;        ssat_vec = 0.9,    &
;        watr     = 0.1,&
;        css_vec  = 4000.0,&
;        cnsd_vec = 0.1

end if

fout->hyds   = hyds/1000.
plot = gsn_csm_contour(wks,fout->hyds,res)
fout->bch    = bch*1.0
plot = gsn_csm_contour(wks,fout->bch,res)
fout->ssat   = ssat*1.0
plot = gsn_csm_contour(wks,fout->ssat,res)
if eq_opt .eq. 0 then
  fout->sucs   = sucs*1.0
else if eq_opt .eq. 1 then
  fout->sucs   = sucs*(-1.0)/1000.
end if
end if
plot = gsn_csm_contour(wks,fout->sucs,res)
fout->swilt  = swilt*1.0
plot = gsn_csm_contour(wks,fout->swilt,res)
fout->sfc    = sfc*1.0
plot = gsn_csm_contour(wks,fout->sfc,res)
fout->css    = css*1.0
plot = gsn_csm_contour(wks,fout->css,res)

if eq_opt .eq. 1 then
  fout->cnsd   = cnsd*1.0
  plot = gsn_csm_contour(wks,fout->cnsd,res)
end if
;delete(fout)

print("finishing gridinfo output")

do layer = 0,5
  fout->hyds_vec(layer,:,:)   = hyds*1.0
  fout->bch_vec(layer,:,:)    = fout->bch*1.0
  fout->sfc_vec(layer,:,:)    = fout->sfc*1.0
  fout->ssat_vec(layer,:,:)   = fout->ssat*1.0
  fout->sucs_vec(layer,:,:)   = fout->sucs*1.0
  fout->swilt_vec(layer,:,:)  = fout->swilt*1.0
  fout->css_vec(layer,:,:)    = fout->css*1.0
  fout->rhosoil_vec(layer,:,:)= fout->rhosoil*1.0
  fout->cnsd_vec(layer,:,:)   = fout->cnsd*1.0
if eq_opt .eq. 1 then
  fout->watr(layer,:,:)       = watr*1.0
end if

end do

; _________________________________ Mask Out ___________________________________
fout->landsea = where( .not. ismissing(fout->sand), fout->landsea, 1.)*1.
fout->landsea = where( .not. ismissing(fout->isoil), fout->landsea, 1.)*1.

do var_num = 0, 31
  print(nml_2D(var_num))
  fout->$nml_2D(var_num)$ = where(fout->landsea .eq. 0, fout->$nml_2D(var_num)$, fout->$nml_2D(var_num)$@_FillValue)
end do
print(nml_3D(0))
fout->$nml_3D(0)$(0,:,:) = where(fout->landsea .eq. 0, fout->$nml_3D(0)$(0,:,:), fout->$nml_3D(0)$@_FillValue)
print(nml_3D(1))
do time = 0,11
  fout->$nml_3D(1)$(time,:,:) = where(fout->landsea .eq. 0, fout->$nml_3D(1)$(time,:,:), fout->$nml_3D(1)$@_FillValue)
end do
do var_num = 2, 16
  print(nml_3D(var_num))
  do layer = 0,5
     fout->$nml_3D(var_num)$(layer,:,:) = where(fout->landsea .eq. 0, fout->$nml_3D(var_num)$(layer,:,:), fout->$nml_3D(var_num)$@_FillValue)
  end do
end do

do var_num = 0,1
  print(nml_4D(var_num))
  do time = 0,11
    do layer = 0,5
        fout->$nml_4D(var_num)$(time,layer,:,:) =\
            where(fout->landsea .eq. 0, fout->$nml_4D(var_num)$(time,layer,:,:), \
            fout->$nml_4D(var_num)$@_FillValue)
    end do
  end do
end do



plot = gsn_csm_contour(wks, dim_avg_n_Wrap(fout->hyds_vec,0),res)
plot = gsn_csm_contour(wks, dim_avg_n_Wrap(fout->bch_vec,0),res)
plot = gsn_csm_contour(wks, dim_avg_n_Wrap(fout->sfc_vec,0),res)
plot = gsn_csm_contour(wks, dim_avg_n_Wrap(fout->ssat_vec,0),res)
plot = gsn_csm_contour(wks, dim_avg_n_Wrap(fout->sucs_vec,0),res)
plot = gsn_csm_contour(wks, dim_avg_n_Wrap(fout->swilt_vec,0),res)
plot = gsn_csm_contour(wks, dim_avg_n_Wrap(fout->css_vec,0),res)
plot = gsn_csm_contour(wks, dim_avg_n_Wrap(fout->rhosoil_vec,0),res)
plot = gsn_csm_contour(wks, dim_avg_n_Wrap(fout->cnsd_vec,0),res)
plot = gsn_csm_contour(wks, dim_avg_n_Wrap(fout->watr,0),res)

print("finishing param_vec output")

end
