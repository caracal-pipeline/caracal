.. caracal documentation master file, created by
   sphinx-quickstart on Mon Feb 18 15:04:26 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
 
.. _inspect:
 
==========================================
inspect
==========================================
 
.. toctree::
   :maxdepth: 1
 
Diagnostic plots for data



.. _inspect_enable:

--------------------------------------------------
**enable**
--------------------------------------------------

  *bool*

  Execute the inspect worker (i.e. diagnostic plotting of the first-pass cross-calibrated data).



.. _inspect_label_in:

--------------------------------------------------
**label_in**
--------------------------------------------------

  *str*, *optional*, *default = ' '*

  Label of the input dataset



.. _inspect_field:

--------------------------------------------------
**field**
--------------------------------------------------

  *str*, *optional*, *default = calibrators*

  Fields that should be inspected. It can be set to 'target', 'calibrators' (i.e., all calibrators) or any comma-separated combination of 'fcal','bpcal','gcal', as defined in the obsconf worker. N$



.. _inspect_label_plot:

--------------------------------------------------
**label_plot**
--------------------------------------------------

  *str*, *optional*, *default = ' '*

  Label for output products (plots etc.) for this step.



.. _inspect_dirname:

--------------------------------------------------
**dirname**
--------------------------------------------------

  *str*, *optional*, *default = ' '*

  Subdirectory (under diagnostic plots) where the plots are to go.



.. _inspect_shadems:

--------------------------------------------------
**shadems**
--------------------------------------------------

  Direct list of shadems plots.

  **enable**

    *bool*, *optional*, *default = True*

    Execute series of "extended" shadems plots.

  **default_column**

    *str*, *optional*, *default = CORRECTED_DATA*

    Data column to plot.

  **plots_by_field**

    *list* *of any*, *optional*, *default = ' '*

    Sequence of shadems plot specifications, made per each field

  **plots_by_corr**

    *list* *of any*, *optional*, *default = ' '*

    Sequence of shadems plot specifications, made per each correlation

  **plots**

    *list* *of any*, *optional*, *default = ' '*

    Sequence of freeform shadems plot specifications

  **ignore_errors**

    *bool*, *optional*, *default = True*

    Don't halt the pipeline for shadems plotting errors.



.. _inspect_standard_plotter:

--------------------------------------------------
**standard_plotter**
--------------------------------------------------

  *{"plotms", "shadems", "ragavi_vis", "none", "None"}*, *optional*, *default = ragavi_vis*

  Application to use for making "standard" plots. Use "none" to disable.



.. _inspect_correlation:

--------------------------------------------------
**correlation**
--------------------------------------------------

  *str*, *optional*, *default = diag*

  Label(s) specifying the correlations. Use the special values 'diag' and 'all' to select only diagonal (paralell hand) or all correlations.



.. _inspect_num_cores:

--------------------------------------------------
**num_cores**
--------------------------------------------------

  *int*, *optional*, *default = 8*

  number of CPUs to use



.. _inspect_mem_limit:

--------------------------------------------------
**mem_limit**
--------------------------------------------------

  *str*, *optional*, *default = 8GB*

  Amount of memory (RAM) to use



.. _inspect_uvrange:

--------------------------------------------------
**uvrange**
--------------------------------------------------

  *str*, *optional*, *default = ' '*

  Set the U-V range for data selection, e.g. '>50'.



.. _inspect_real_imag:

--------------------------------------------------
**real_imag**
--------------------------------------------------

  Plot real vs imaginary parts of data.

  **enable**

    *bool*, *optional*, *default = False*

    Executed the real v/s imaginary data plotting.

  **col**

    *str*, *optional*, *default = corrected*

    Data column to plot.

  **avgtime**

    *str*, *optional*, *default = 10*

    Time to average for plotting, in seconds.

  **avgchan**

    *str*, *optional*, *default = 10*

    Number of channels to average for plotting.



.. _inspect_amp_phase:

--------------------------------------------------
**amp_phase**
--------------------------------------------------

  Plot Amplitude vs Phase for  data.

  **enable**

    *bool*, *optional*, *default = False*

    Executes the plotting of amplitude v/s phase for data.

  **col**

    *str*, *optional*, *default = corrected*

    Data column to plot.

  **avgtime**

    *str*, *optional*, *default = 10*

    Time to average for plotting, in seconds.

  **avgchan**

    *str*, *optional*, *default = 10*

    Number of channels to average for plotting.



.. _inspect_amp_uvwave:

--------------------------------------------------
**amp_uvwave**
--------------------------------------------------

  Plot data amplitude v/s  uvwave.

  **enable**

    *bool*, *optional*, *default = False*

    Executes plotting data amplitude as a function of uvwave.

  **col**

    *str*, *optional*, *default = corrected*

    Data column to plot.

  **avgtime**

    *str*, *optional*, *default = 10*

    Time to average for plotting, in seconds.

  **avgchan**

    *str*, *optional*, *default = 10*

    Number of channels to average for plotting.



.. _inspect_amp_ant:

--------------------------------------------------
**amp_ant**
--------------------------------------------------

  Plot data amplitde v/s antenna.

  **enable**

    *bool*, *optional*, *default = False*

    Executes plotting data amplitude v/s antennas.

  **col**

    *str*, *optional*, *default = corrected*

    Data column to plot.

  **avgtime**

    *str*, *optional*, *default = 10*

    Time to average for plotting, in seconds.

  **avgchan**

    *str*, *optional*, *default = 10*

    Number of channels to average for plotting.



.. _inspect_phase_uvwave:

--------------------------------------------------
**phase_uvwave**
--------------------------------------------------

  Plot data phase v/s uvwave.

  **enable**

    *bool*, *optional*, *default = False*

    Executes plotting data phase v/s uvwave.

  **col**

    *str*, *optional*, *default = corrected*

    Data column to plot.

  **avgtime**

    *str*, *optional*, *default = 10*

    Time to average for plotting, in seconds.

  **avgchan**

    *str*, *optional*, *default = 10*

    Number of channels to average for plotting.



.. _inspect_amp_scan:

--------------------------------------------------
**amp_scan**
--------------------------------------------------

  Plot data amplitude v/s scan number.

  **enable**

    *bool*, *optional*, *default = False*

    Executes plotting data amplitude v/s scan number.

  **col**

    *str*, *optional*, *default = corrected*

    Data column to plot.

  **avgtime**

    *str*, *optional*, *default = 10*

    Time to average for plotting, in seconds.

  **avgchan**

    *str*, *optional*, *default = 10*

    Number of channels to average for plotting.



.. _inspect_amp_chan:

--------------------------------------------------
**amp_chan**
--------------------------------------------------

  Plot Amplitude vs Channel data.

  **enable**

    *bool*, *optional*, *default = False*

    Executes the plotting of amplitude v/s phase for data.

  **col**

    *str*, *optional*, *default = corrected*

    Data column to plot.

  **avgtime**

    *str*, *optional*, *default = 10*

    Time to average for plotting, in seconds.

  **avgchan**

    *str*, *optional*, *default = 10*

    Number of channels to average for plotting.



.. _inspect_phase_chan:

--------------------------------------------------
**phase_chan**
--------------------------------------------------

  Plot Phase vs Chan.

  **enable**

    *bool*, *optional*, *default = False*

    Executes the plotting of amplitude v/s phase for data.

  **col**

    *str*, *optional*, *default = corrected*

    Data column to plot.

  **avgtime**

    *str*, *optional*, *default = 10*

    Time to average for plotting, in seconds.

  **avgchan**

    *str*, *optional*, *default = 10*

    Number of channels to average for plotting.



.. _inspect_report:

--------------------------------------------------
**report**
--------------------------------------------------

  *bool*, *optional*, *default = False*

  (Re)generate a full HTML report at the end of this worker.



.. _inspect_cabs:

--------------------------------------------------
**cabs**
--------------------------------------------------

  *list* *of map*, *optional*, *default = ' '*

  Specifies non-default image versions and/or tags for Stimela cabs. Running with scissors: use with extreme caution.

